/************************************************************************************
    Copyright (C) 2018 MariaDB Corpoeation AB

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.

   You should have received a copy of the GNU Library General Public
   License along with this library; if not see <http://www.gnu.org/licenses>
   or write to the Free Software Foundation, Inc.,
   51 Franklin St., Fifth Floor, Boston, MA 02110, USA

*************************************************************************************/

#include <ma_global.h>
#include <ma_sys.h>
#include <mysql.h>
#include <errmsg.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <zlib.h>
#include <mariadb_rpl.h>

static int rpl_alloc_string(MARIADB_RPL_EVENT *event,
                            MARIADB_STRING *s,
                            unsigned char *buffer,
                            size_t len)
{
  if (!(s->str= ma_alloc_root(&event->memroot, len)))
    return 1;
  memcpy(s->str, buffer, len);
  s->length= len;
  return 0;
}

MARIADB_RPL * STDCALL mariadb_rpl_init_ex(MYSQL *mysql, unsigned int version)
{
  MARIADB_RPL *rpl;

  if (version < MARIADB_RPL_REQUIRED_VERSION ||
      version > MARIADB_RPL_VERSION)
  {
    my_set_error(mysql, CR_VERSION_MISMATCH, SQLSTATE_UNKNOWN, 0, version,
                     MARIADB_RPL_VERSION, MARIADB_RPL_REQUIRED_VERSION);
    return 0;
  }

  if (!mysql)
    return NULL;

  if (!(rpl= (MARIADB_RPL *)calloc(1, sizeof(MARIADB_RPL))))
  {
    SET_CLIENT_ERROR(mysql, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
    return 0;
  }
  rpl->version= version;
  rpl->mysql= mysql;
  return rpl;
}

void STDCALL mariadb_free_rpl_event(MARIADB_RPL_EVENT *event)
{
  if (event)
  {
    ma_free_root(&event->memroot, MYF(0));
    free(event);
  }
}

int STDCALL mariadb_rpl_open(MARIADB_RPL *rpl)
{
  unsigned char *ptr, *buf;
  if (!rpl || !rpl->mysql)
    return 1;

  /* COM_BINLOG_DUMP:
     Ofs  Len Data
     0      1 COM_BINLOG_DUMP
     1      4 position
     5      2 flags
     7      4 server id
     11     * filename

     * = filename length

  */
  ptr= buf= 
#ifdef WIN32
          (unsigned char *)_alloca(rpl->filename_length + 11);
#else
	  (unsigned char *)alloca(rpl->filename_length + 11);
#endif

  int4store(ptr, (unsigned int)rpl->start_position);
  ptr+= 4;
  int2store(ptr, rpl->flags);
  ptr+= 2;
  int4store(ptr, rpl->server_id);
  ptr+= 4;
  memcpy(ptr, rpl->filename, rpl->filename_length);
  ptr+= rpl->filename_length;

  if (ma_simple_command(rpl->mysql, COM_BINLOG_DUMP, (const char *)buf, ptr - buf, 1, 0))
    return 1;
  return 0;
}

MARIADB_RPL_EVENT * STDCALL mariadb_rpl_fetch(MARIADB_RPL *rpl, MARIADB_RPL_EVENT *event)
{
  unsigned char *ev;
  size_t len;
  MARIADB_RPL_EVENT *rpl_event= 0;

  if (!rpl || !rpl->mysql)
    return 0;

  while (1) {
    unsigned long pkt_len= ma_net_safe_read(rpl->mysql);

    if (pkt_len == packet_error)
    {
      rpl->buffer_size= 0;
      return 0;
    }

    /* EOF packet:
       see https://mariadb.com/kb/en/library/eof_packet/
       Packet length must be less than 9 bytes, EOF header
       is 0xFE.
    */
    if (pkt_len < 9 && rpl->mysql->net.read_pos[0] == 0xFE)
    {
      rpl->buffer_size= 0;
      return 0;
    }

    /* if ignore heartbeat flag was set, we ignore this
       record and continue to fetch next record.
       The first byte is always status byte (0x00)
       For event header description see
       https://mariadb.com/kb/en/library/2-binlog-event-header/ */
    if (rpl->flags & MARIADB_RPL_IGNORE_HEARTBEAT)
    {
      if (rpl->mysql->net.read_pos[1 + 4] == HEARTBEAT_LOG_EVENT)
        continue;
    }

    rpl->buffer_size= pkt_len;
    rpl->buffer= rpl->mysql->net.read_pos;

    if (event)
    {
      MA_MEM_ROOT memroot= event->memroot;
      rpl_event= event;
      ma_free_root(&memroot, MYF(MY_KEEP_PREALLOC));
      memset(rpl_event, 0, sizeof(MARIADB_RPL_EVENT));
      rpl_event->memroot= memroot;
    } else {
      if (!(rpl_event = (MARIADB_RPL_EVENT *)malloc(sizeof(MARIADB_RPL_EVENT))))
        goto mem_error;
      memset(rpl_event, 0, sizeof(MARIADB_RPL_EVENT));
      ma_init_alloc_root(&rpl_event->memroot, 8192, 0);
    }
    rpl_event->checksum= uint4korr(rpl->buffer + rpl->buffer_size - 4);

    rpl_event->ok= rpl->buffer[0];
    rpl_event->timestamp= uint4korr(rpl->buffer + 1);
    rpl_event->event_type= (unsigned char)*(rpl->buffer + 5);
    rpl_event->server_id= uint4korr(rpl->buffer + 6);
    rpl_event->event_length= uint4korr(rpl->buffer + 10);
    rpl_event->next_event_pos= uint4korr(rpl->buffer + 14);
    rpl_event->flags= uint2korr(rpl->buffer + 18);

    ev= rpl->buffer + EVENT_HEADER_OFS;

    if (rpl->use_checksum)
    {
      rpl_event->checksum= *(ev + rpl_event->event_length - 4);
      rpl_event->event_length-= 4;
    }

    switch(rpl_event->event_type) {
    case HEARTBEAT_LOG_EVENT:
      rpl_event->event.heartbeat.timestamp= uint4korr(ev);
      ev+= 4;
      rpl_event->event.heartbeat.next_position= uint4korr(ev);
      ev+= 4;
      rpl_event->event.heartbeat.type= (uint8_t)*ev;
      ev+= 1;
      rpl_event->event.heartbeat.flags= uint2korr(ev);
      break;
    case BINLOG_CHECKPOINT_EVENT:
      len= uint4korr(ev);
      ev+= 4;
      if (rpl_alloc_string(rpl_event, &rpl_event->event.checkpoint.filename, ev, len))
        goto mem_error;
      break;
    case FORMAT_DESCRIPTION_EVENT:
      rpl_event->event.format_description.format = uint2korr(ev);
      ev+= 2;
      rpl_event->event.format_description.server_version = (char *)(ev);
      ev+= 50;
      rpl_event->event.format_description.timestamp= uint4korr(ev);
      ev+= 4;
      rpl->fd_header_len= rpl_event->event.format_description.header_len= (uint8_t)*ev;
      ev= rpl->buffer + rpl->buffer_size - 5;
      rpl->use_checksum= *ev;
      break;
    case QUERY_EVENT:
    {
      size_t db_len, status_len;
      rpl_event->event.query.thread_id= uint4korr(ev);
      ev+= 4;
      rpl_event->event.query.seconds= uint4korr(ev);
      ev+= 4;
      db_len= *ev;
      ev++;
      rpl_event->event.query.errornr= uint2korr(ev);
      ev+= 2;
      status_len= uint2korr(ev);
      ev+= 2;
      if (rpl_alloc_string(rpl_event, &rpl_event->event.query.status, ev, status_len))
        goto mem_error;
      ev+= status_len;

      if (rpl_alloc_string(rpl_event, &rpl_event->event.query.database, ev, db_len))
        goto mem_error;
      ev+= db_len + 1; /* zero terminated */

      /* calculate statement size: buffer + buffer_size - current_ofs (ev) - crc_size */
      len= (size_t)(rpl->buffer + rpl->buffer_size - ev - 4);
      if (rpl_alloc_string(rpl_event, &rpl_event->event.query.statement, ev, len))
        goto mem_error;
      break;
    }
    case TABLE_MAP_EVENT:
      rpl_event->event.table_map.table_id= uint6korr(ev);
      ev+= 8;
      len= *ev;
      ev++;
      if (rpl_alloc_string(rpl_event, &rpl_event->event.table_map.database, ev, len))
        goto mem_error;
      ev+= len + 1;
      len= *ev;
      ev++;
      if (rpl_alloc_string(rpl_event, &rpl_event->event.table_map.table, ev, len))
        goto mem_error;
      ev+= len + 1;
      rpl_event->event.table_map.column_count= mysql_net_field_length(&ev);
      len= rpl_event->event.table_map.column_count;
      if (rpl_alloc_string(rpl_event, &rpl_event->event.table_map.column_types, ev, len))
        goto mem_error;
      ev+= len;
      len= mysql_net_field_length(&ev);
      if (rpl_alloc_string(rpl_event, &rpl_event->event.table_map.metadata, ev, len))
        goto mem_error;
      break;
    case RAND_EVENT:
      rpl_event->event.rand.first_seed= uint8korr(ev);
      ev+= 8;
      rpl_event->event.rand.second_seed= uint8korr(ev);
      break;
    case INTVAR_EVENT:
      rpl_event->event.intvar.type= *ev;
      ev++;
      rpl_event->event.intvar.value= uint8korr(ev);
      break;
    case USER_VAR_EVENT:
      len= uint4korr(ev);
      ev+= 4;
      if (rpl_alloc_string(rpl_event, &rpl_event->event.uservar.name, ev, len))
        goto mem_error;
      ev+= len;
      if (!(rpl_event->event.uservar.is_null= (uint8)*ev)) 
      {
        ev++;
        rpl_event->event.uservar.type= *ev;
        ev++;
        rpl_event->event.uservar.charset_nr= uint4korr(ev);
        ev+= 4;
        len= uint4korr(ev);
        ev+= 4;
        if (rpl_alloc_string(rpl_event, &rpl_event->event.uservar.value, ev, len))
          goto mem_error;
        ev+= len;
        if ((unsigned long)(ev - rpl->buffer) < rpl->buffer_size)
          rpl_event->event.uservar.flags= *ev;
      }
      break;
    case START_ENCRYPTION_EVENT:
      rpl_event->event.encryption.scheme= *ev;
      ev++;
      rpl_event->event.encryption.key_version= uint4korr(ev);
      ev+= 4;
      rpl_event->event.encryption.nonce= (char *)ev;
      break;
    case ANNOTATE_ROWS_EVENT:
      len= (uint32)(rpl->buffer + rpl->buffer_size - (unsigned char *)ev - 4);
      if (rpl_alloc_string(rpl_event, &rpl_event->event.annotate_rows.statement, ev, len))
        goto mem_error;
      break;
    case ROTATE_EVENT:
      rpl_event->event.rotate.position= uint8korr(ev);
      ev+= 8;
      len= rpl_event->event_length - rpl->fd_header_len - 8;
      if (rpl_alloc_string(rpl_event, &rpl_event->event.rotate.filename, ev, len))
        goto mem_error;
      break;
    case XID_EVENT:
      rpl_event->event.xid.transaction_nr= uint8korr(ev);
      break;
    case STOP_EVENT:
      /* nothing to do here */
      break;
    case GTID_EVENT:
      rpl_event->event.gtid.sequence_nr= uint8korr(ev);
      ev+= 8;
      rpl_event->event.gtid.domain_id= uint4korr(ev);
      ev+= 4;
      rpl_event->event.gtid.flags= *ev;
      ev++;
      if (rpl_event->event.gtid.flags & FL_GROUP_COMMIT_ID)
        rpl_event->event.gtid.commit_id= uint8korr(ev);
      break;
    case GTID_LIST_EVENT:
    {
      uint32 i;
      rpl_event->event.gtid_list.gtid_cnt= uint4korr(ev);
      ev++;
      if (!(rpl_event->event.gtid_list.gtid= (MARIADB_GTID *)ma_alloc_root(&rpl_event->memroot, sizeof(MARIADB_GTID) * rpl_event->event.gtid_list.gtid_cnt)))
        goto mem_error;
      for (i=0; i < rpl_event->event.gtid_list.gtid_cnt; i++)
      {
        rpl_event->event.gtid_list.gtid[i].domain_id= uint4korr(ev);
        ev+= 4;
        rpl_event->event.gtid_list.gtid[i].server_id= uint4korr(ev);
        ev+= 4;
        rpl_event->event.gtid_list.gtid[i].sequence_nr= uint8korr(ev);
        ev+= 8;
      }
      break;
    }
    case WRITE_ROWS_EVENT_V1:
    case UPDATE_ROWS_EVENT_V1:
    case DELETE_ROWS_EVENT_V1:
      rpl_event->event.rows.type= rpl_event->event_type - WRITE_ROWS_EVENT_V1;
      if (rpl->fd_header_len == 6)
      {
        rpl_event->event.rows.table_id= uint4korr(ev);
        ev+= 4;
      } else {
        rpl_event->event.rows.table_id= uint6korr(ev);
        ev+= 6;
      }
      rpl_event->event.rows.flags= uint2korr(ev);
      ev+= 2;
      len= rpl_event->event.rows.column_count= mysql_net_field_length(&ev);
      if (!len)
        break;
      if (!(rpl_event->event.rows.column_bitmap =
            (char *)ma_alloc_root(&rpl_event->memroot, (len + 7) / 8)))
        goto mem_error;
      memcpy(rpl_event->event.rows.column_bitmap, ev, (len + 7) / 8);
      ev+= (len + 7) / 8;
      if (rpl_event->event_type == UPDATE_ROWS_EVENT_V1)
      {
        if (!(rpl_event->event.rows.column_update_bitmap =
            (char *)ma_alloc_root(&rpl_event->memroot, (len + 7) / 8)))
          goto mem_error;
        memcpy(rpl_event->event.rows.column_update_bitmap, ev, (len + 7) / 8);
        ev+= (len + 7) / 8;
      }
      len= (rpl->buffer + rpl_event->event_length + EVENT_HEADER_OFS - rpl->fd_header_len) - ev;
      if ((rpl_event->event.rows.row_data_size= len))
      {
        if (!(rpl_event->event.rows.row_data =
            (char *)ma_alloc_root(&rpl_event->memroot, rpl_event->event.rows.row_data_size)))
        goto mem_error;
        memcpy(rpl_event->event.rows.row_data, ev, rpl_event->event.rows.row_data_size);
      }
      break;
    default:
      return NULL;
      break;
    }
    return rpl_event;
  }
mem_error:
  SET_CLIENT_ERROR(rpl->mysql, CR_OUT_OF_MEMORY, SQLSTATE_UNKNOWN, 0);
  return 0;
}

void STDCALL mariadb_rpl_close(MARIADB_RPL *rpl)
{
  if (!rpl)
    return;
  if (rpl->filename)
    free((void *)rpl->filename);
  free(rpl);
  return;
}

int STDCALL mariadb_rpl_optionsv(MARIADB_RPL *rpl,
                        enum mariadb_rpl_option option,
                        ...)
{
  va_list ap;
  int rc= 0;

  if (!rpl)
    return 1;

  va_start(ap, option);

  switch (option) {
  case MARIADB_RPL_FILENAME:
  {
    char *arg1= va_arg(ap, char *);
    rpl->filename_length= (uint32_t)va_arg(ap, size_t);
    free((void *)rpl->filename);
    rpl->filename= NULL;
    if (rpl->filename_length)
    {
      rpl->filename= (char *)malloc(rpl->filename_length);
      memcpy((void *)rpl->filename, arg1, rpl->filename_length);
    }
    else if (arg1)
    {
      rpl->filename= strdup((const char *)arg1);
      rpl->filename_length= (uint32_t)strlen(rpl->filename);
    }
    break;
  }
  case MARIADB_RPL_SERVER_ID:
  {
    rpl->server_id= va_arg(ap, unsigned int);
    break;
  }
  case MARIADB_RPL_FLAGS:
  {
    rpl->flags= va_arg(ap, unsigned int);
    break;
  }
  case MARIADB_RPL_START:
  {
    rpl->start_position= va_arg(ap, unsigned long);
    break;
  }
  default:
    rc= -1;
    goto end;
  }
end:
  return rc;
}

int STDCALL mariadb_rpl_get_optionsv(MARIADB_RPL *rpl,
                        enum mariadb_rpl_option option,
                        ...)
{
  va_list ap;

  if (!rpl)
    return 1;

  va_start(ap, option);

  switch (option) {
  case MARIADB_RPL_FILENAME:
  {
    const char **name= (const char **)va_arg(ap, char **);
    size_t *len= (size_t*)va_arg(ap, size_t *);

    *name= rpl->filename;
    *len= rpl->filename_length;
    break;
  }
  case MARIADB_RPL_SERVER_ID:
  {
    unsigned int *id= va_arg(ap, unsigned int *);
    *id= rpl->server_id;
    break;
  }
  case MARIADB_RPL_FLAGS:
  {
    unsigned int *flags= va_arg(ap, unsigned int *);
    *flags= rpl->flags;
    break;
  }
  case MARIADB_RPL_START:
  {
    unsigned long *start= va_arg(ap, unsigned long *);
    *start= rpl->start_position;
    break;
  }
  default:
    return 1;
    break;
  }
  return 0;
}
