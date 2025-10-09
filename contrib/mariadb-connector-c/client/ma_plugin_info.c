
#include <my_global.h>
#include <my_sys.h>
#include <mysql.h>
#include <mysql/client_plugin.h>
#include <getopt.h>
#include <stdio.h>
#include <my_dir.h>
#include <ma_string.h>

#define CLIENT_PLUGIN_INFO_VERSION "1.0.0"

static struct option long_options[]=
{
  {"all", no_argument, 0, 'a'},
  {"builtin", no_argument, 0, 'b'},
  {"dynamic", no_argument, 0, 'd'},
  {"directory", 1, 0, 'p'},
  {"plugin_name", 1, 0, 'n'},
  {"version", no_argument, 0, 'v'},
  {"help", no_argument, 0, '?'},
  {NULL, 0, 0, 0}
};

static char *values[] =
{
  "show information for all plugins",
  "show information for builtin plugins",
  "show information for dynamic plugins",
  "show information for dynamic plugins in specified directory",
  "show information for specified plugin",
  "show version information",
  "display this help and exit",
  NULL
};

struct st_plugin_type
{
  int type;
  char *typename;
};

#ifndef _WIN32
int my_errno=0;
#endif

static struct st_plugin_type plugin_types[]=
{
  {MYSQL_CLIENT_AUTHENTICATION_PLUGIN, "authentication"},
  {MARIADB_CLIENT_PVIO_PLUGIN, "virtual IO"},
  {MARIADB_CLIENT_TRACE_PLUGIN, "trace"},
  {MARIADB_CLIENT_REMOTEIO_PLUGIN, "remote file access"},
  {MARIADB_CLIENT_CONNECTION_PLUGIN, "connection handler"},
  {0, "unknown"}
};

static void version()
{
  printf("%s Version %s\n", ma_progname, CLIENT_PLUGIN_INFO_VERSION);
}

static void usage(void)
{
  int i=0;
  printf("%s Version %s\n", ma_progname, CLIENT_PLUGIN_INFO_VERSION);
  puts("Copyright 2015 MariaDB Corporation AB");
  puts("Show client plugin information for MariaDB Connector/C.");
  printf("Usage: %s [OPTIONS] [plugin_name]\n", ma_progname);
  while (long_options[i].name)
  {
    printf("  --%-12s -%s\n", long_options[i].name, values[i]);
    i++;
  }
}

static char *ma_get_type_name(int type)
{
  int i=0;
  while (plugin_types[i].type)
  {
    if (type== plugin_types[i].type)
      return plugin_types[i].typename;
    i++;
  }
  return plugin_types[i].typename;
}

static void show_plugin_info(struct st_mysql_client_plugin *plugin, my_bool builtin)
{
  printf("Name: %s\n", plugin->name);
  printf("Type: %s\n", ma_get_type_name(plugin->type));
  printf("Desc: %s\n", plugin->desc);
  printf("Author: %s\n", plugin->author);
  printf("License: %s\n", plugin->license);
  printf("Version: %d.%d.%d\n", plugin->version[0], plugin->version[1], plugin->version[2]);
  printf("API Version: 0x%04X\n", plugin->interface_version);
  printf("Build type: %s\n", builtin ? "builtin" : "dynamic");
  printf("\n");
}

static void show_builtin()
{
  struct st_mysql_client_plugin **builtin;

  for (builtin= mysql_client_builtins; *builtin; builtin++)
    show_plugin_info(*builtin, TRUE);
}

static void show_file(char *filename)
{
  char dlpath[FN_REFLEN+1];
  void *sym, *dlhandle;
  struct st_mysql_client_plugin *plugin;
  char *env_plugin_dir= getenv("MARIADB_PLUGIN_DIR");
  char *has_so_ext= strstr(filename, SO_EXT);

  if (!strchr(filename, FN_LIBCHAR))
    snprintf(dlpath, sizeof(dlpath) - 1, "%s/%s%s",
             (env_plugin_dir) ? env_plugin_dir : PLUGINDIR, 
             filename, 
             has_so_ext ? "" : SO_EXT);
  else
    strcpy(dlpath, filename);
  if ((dlhandle= dlopen((const char *)dlpath, RTLD_NOW)))
  {
    if (sym= dlsym(dlhandle, plugin_declarations_sym))
    {
      plugin= (struct st_mysql_client_plugin *)sym;
      show_plugin_info(plugin, 0);
    }
    dlclose(dlhandle);
  }
}

static void show_dynamic(const char *directory)
{
  MY_DIR *dir= NULL;
  unsigned int i;
  char *plugin_dir= directory ? (char *)directory : getenv("MARIADB_PLUGIN_DIR");

  if (!plugin_dir)
    plugin_dir= PLUGINDIR;

  printf("plugin_dir %s\n", plugin_dir);

  dir= my_dir(plugin_dir, 0);

  if (!dir || !dir->number_off_files)
  {
    printf("No plugins found in %s\n", plugin_dir);
    goto end;
  }

  for (i=0; i < dir->number_off_files; i++)
  {
    char *p= strstr(dir->dir_entry[i].name, SO_EXT);
    if (p)
      show_file(dir->dir_entry[i].name);
  }
end:
  if (dir)
    my_dirend(dir);
}

int main(int argc, char *argv[])
{
  int option_index= 0;
  int c;
  ma_progname= argv[0];

  mysql_server_init(0, NULL, NULL);

  if (argc <= 1)
  {
    usage();
    exit(1);
  }

  c= getopt_long(argc, argv, "bdapnvh?", long_options, &option_index);

  switch(c) {
  case 'a': /* all */
    show_builtin();
    show_dynamic(NULL);
    break;
  case 'b': /* builtin */
    show_builtin();
    break;
  case 'd': /* dynamic */
    show_dynamic(NULL);
    break;
  case 'v':
    version();
    break;
  case 'n':
    if (argc > 2)
      show_file(argv[2]);
    break;
  case 'p':
    if (argc > 2)
      show_dynamic(argv[2]);
    break;
  case '?':
    usage();
    break;
  default:
    printf("unrecocognized option: %s", argv[1]);
    exit(1);
  }
  exit(0);
}
