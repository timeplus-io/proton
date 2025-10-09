#!/usr/bin/env python
#
# Copyright 2017-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""IDL for functions that take flexible options as a bson_t.

Defines the options accepted by functions that receive a const bson_t *opts,
for example mongoc_collection_find_with_opts, mongoc_collection_insert_one,
and many others.

Generates struct types, options parsing code, and RST documentation.

Written for Python 2.6+, requires Jinja 2 for templating.
"""

# yapf: disable
from collections import OrderedDict
from os.path import basename, dirname, join as joinpath, normpath
import re

from jinja2 import Environment, FileSystemLoader  # Please "pip install jinja2".

this_dir = dirname(__file__)
template_dir = joinpath(this_dir, 'opts_templates')
src_dir = normpath(joinpath(this_dir, '../src/libmongoc/src/mongoc'))
doc_includes = normpath(joinpath(this_dir, '../src/libmongoc/doc/includes'))


def flatten(items):
    for item in items:
        if isinstance(item, list):
            # "yield from".
            for subitem in flatten(item):
                yield subitem
        else:
            yield item


class Struct(OrderedDict):
    def __init__(self, items, opts_name='opts', generate_rst=True,
                 generate_code=True, allow_extra=True, **defaults):
        """Define an options struct.

        - items: List of pairs: (optionName, info)
        - opts_name: Name of the const bson_t *opts parameter
        - allow_extra: Whether to allow unrecognized options
        - defaults: Initial values for options
        """
        OrderedDict.__init__(self, list(flatten(items)))
        self.is_shared = False
        self.opts_name = opts_name
        self.generate_rst = generate_rst
        self.generate_code = generate_code
        self.allow_extra = allow_extra
        self.defaults = defaults

    def default(self, item, fallback):
        return self.defaults.get(item, fallback)


class Shared(Struct):
    def __init__(self, items, **defaults):
        """Define a struct that is shared by others."""
        super(Shared, self).__init__(items, **defaults)
        self.is_shared = True
        self.generate_rst = False

read_concern_help = 'Construct a :symbol:`mongoc_read_concern_t` and use :symbol:`mongoc_read_concern_append` to add the read concern to ``opts``. See the example code for :symbol:`mongoc_client_read_command_with_opts`. Read concern requires MongoDB 3.2 or later, otherwise an error is returned.'
read_concern_document_option = ('readConcern', {
    'type': 'document',
    'help': read_concern_help
})

read_concern_option = ('readConcern', {
    'type': 'mongoc_read_concern_t *',
    'help': read_concern_help,
    'convert': '_mongoc_convert_read_concern'
})

write_concern_option = [
    ('writeConcern', {
        'type': 'mongoc_write_concern_t *',
        'convert': '_mongoc_convert_write_concern',
        'help': 'Construct a :symbol:`mongoc_write_concern_t` and use :symbol:`mongoc_write_concern_append` to add the write concern to ``opts``. See the example code for :symbol:`mongoc_client_write_command_with_opts`.'
    }),
    ('write_concern_owned', {
        'type': 'bool',
        'internal': True,
    })
]

session_option = ('sessionId', {
    'type': 'mongoc_client_session_t *',
    'convert': '_mongoc_convert_session_id',
    'field': 'client_session',
    'help': 'First, construct a :symbol:`mongoc_client_session_t` with :symbol:`mongoc_client_start_session`. You can begin a transaction with :symbol:`mongoc_client_session_start_transaction`, optionally with a :symbol:`mongoc_transaction_opt_t` that overrides the options inherited from |opts-source|, and use :symbol:`mongoc_client_session_append` to add the session to ``opts``. See the example code for :symbol:`mongoc_client_session_t`.'
})

ordered_option = ('ordered', {
    'type': 'bool',
    'help': 'set to ``false`` to attempt to insert all documents, continuing after errors.'
})

validate_option = ('validate', {
    'type': 'bson_validate_flags_t',
    'convert': '_mongoc_convert_validate_flags',
    'help': 'Construct a bitwise-or of all desired :symbol:`bson_validate_flags_t <bson_validate_with_error>`. Set to ``false`` to skip client-side validation of the provided BSON documents.'
})

collation_option = ('collation', {
    'type': 'document',
    'help': 'Configure textual comparisons. See `Setting Collation Order <setting_collation_order_>`_, and `the MongoDB Manual entry on Collation <https://www.mongodb.com/docs/manual/reference/collation/>`_. Collation requires MongoDB 3.2 or later, otherwise an error is returned.'
})

array_filters_option = ('arrayFilters', {
    'type': 'array',
    'help': 'An array of filters specifying to which array elements an update should apply.',
})

upsert_option = ('upsert', {
    'type': 'bool',
    'help': 'When true, creates a new document if no document matches the query.'
})

bypass_option = ('bypassDocumentValidation', {
    'type': 'bool',
    'field': 'bypass',
    'help': 'Set to ``true`` to skip server-side schema validation of the provided BSON documents.'
})

server_option = ('serverId', {
    'type': 'uint32_t',
    'convert': '_mongoc_convert_server_id',
    'help': 'To target a specific server, include an int32 "serverId" field. Obtain the id by calling :symbol:`mongoc_client_select_server`, then :symbol:`mongoc_server_description_id` on its return value.'
})

hint_option = ('hint', {
    'type': 'bson_value_t',
    'convert': '_mongoc_convert_hint',
    'help': 'A document or string that specifies the index to use to support the query predicate.'
})

let_option = ('let', {
    'type': 'document',
    'help': 'A BSON document consisting of any number of parameter names, each followed by definitions of constants in the MQL Aggregate Expression language.'
})

comment_option_since_4_4 = ('comment', {
    'type': 'bson_value_t',
    'convert': '_mongoc_convert_bson_value_t',
    'help': 'A :symbol:`bson_value_t` specifying the comment to attach to this command. The comment will appear in log messages, profiler output, and currentOp output. Requires MongoDB 4.4 or later.'
})

comment_option_string_pre_4_4 = ('comment', {
    'type': 'bson_value_t',
    'convert': '_mongoc_convert_bson_value_t',
    'help': 'A :symbol:`bson_value_t` specifying the comment to attach to this command. The comment will appear in log messages, profiler output, and currentOp output. Only string values are supported prior to MongoDB 4.4.'
})

opts_structs = OrderedDict([
    ('mongoc_crud_opts_t', Shared([
        write_concern_option,
        session_option,
        validate_option,
        comment_option_since_4_4,
    ])),

    ('mongoc_update_opts_t', Shared([
        ('crud', {'type': 'mongoc_crud_opts_t'}),
        bypass_option,
        collation_option,
        hint_option,
        upsert_option,
        let_option,
    ])),

    ('mongoc_insert_one_opts_t', Struct([
        ('crud', {'type': 'mongoc_crud_opts_t'}),
        bypass_option
    ], validate='_mongoc_default_insert_vflags')),

    ('mongoc_insert_many_opts_t', Struct([
        ('crud', {'type': 'mongoc_crud_opts_t'}),
        ordered_option,
        bypass_option,
    ], validate='_mongoc_default_insert_vflags', ordered='true')),

    ('mongoc_delete_opts_t', Shared([
        ('crud', {'type': 'mongoc_crud_opts_t'}),
        collation_option,
        hint_option,
        let_option,
    ])),

    ('mongoc_delete_one_opts_t', Struct([
        ('delete', {'type': 'mongoc_delete_opts_t'}),
    ])),

    ('mongoc_delete_many_opts_t', Struct([
        ('delete', {'type': 'mongoc_delete_opts_t'}),
    ])),

    ('mongoc_update_one_opts_t', Struct([
        ('update', {'type': 'mongoc_update_opts_t'}),
        array_filters_option,
    ], validate='_mongoc_default_update_vflags')),

    ('mongoc_update_many_opts_t', Struct([
        ('update', {'type': 'mongoc_update_opts_t'}),
        array_filters_option,
    ], validate='_mongoc_default_update_vflags')),

    ('mongoc_replace_one_opts_t', Struct([
        ('update', {'type': 'mongoc_update_opts_t'}),
    ], validate='_mongoc_default_replace_vflags')),

    ('mongoc_bulk_opts_t', Struct([
        write_concern_option,
        ordered_option,
        session_option,
        let_option,
        comment_option_since_4_4,
    ], allow_extra=False, ordered='true')),

    ('mongoc_bulk_insert_opts_t', Struct([
        validate_option,
    ], validate='_mongoc_default_insert_vflags', allow_extra=False)),

    ('mongoc_bulk_update_opts_t', Shared([
        validate_option,
        collation_option,
        hint_option,
        ('upsert', {
            'type': 'bool',
            'help': 'If true, insert a document if none match ``selector``.'
        }),
        ('multi', {'type': 'bool', 'hidden': True})
    ])),

    ('mongoc_bulk_update_one_opts_t', Struct(
        [
            ('update', {'type': 'mongoc_bulk_update_opts_t'}),
            array_filters_option,
        ],
        multi='false',
        validate='_mongoc_default_update_vflags',
        allow_extra=False)),

    ('mongoc_bulk_update_many_opts_t', Struct(
        [
            ('update', {'type': 'mongoc_bulk_update_opts_t'}),
            array_filters_option,
        ],
        multi='true',
        validate='_mongoc_default_update_vflags',
        allow_extra=False)),

    ('mongoc_bulk_replace_one_opts_t', Struct(
        [('update', {'type': 'mongoc_bulk_update_opts_t'})],
        multi='false',
        validate='_mongoc_default_replace_vflags',
        allow_extra=False)),

    ('mongoc_bulk_remove_opts_t', Shared([
        collation_option,
        hint_option,
        ('limit', {'type': 'int32_t', 'hidden': True})
    ])),

    ('mongoc_bulk_remove_one_opts_t', Struct([
        ('remove', {'type': 'mongoc_bulk_remove_opts_t'}),
    ], limit=1, allow_extra=False)),

    ('mongoc_bulk_remove_many_opts_t', Struct([
        ('remove', {'type': 'mongoc_bulk_remove_opts_t'}),
    ], limit=0, allow_extra=False)),

    ('mongoc_change_stream_opts_t', Struct([
        ('batchSize', {'type': 'int32_t', 'help': 'An ``int32`` representing number of documents requested to be returned on each call to :symbol:`mongoc_change_stream_next`'}),
        ('resumeAfter', {'type': 'document', 'help': 'A ``Document`` representing the logical starting point of the change stream. The result of :symbol:`mongoc_change_stream_get_resume_token()` or the ``_id`` field  of any change received from a change stream can be used here. This option is mutually exclusive with ``startAfter`` and ``startAtOperationTime``.'}),
        ('startAfter', {'type': 'document', 'help': 'A ``Document`` representing the logical starting point of the change stream. Unlike ``resumeAfter``, this can resume notifications after an "invalidate" event. The result of :symbol:`mongoc_change_stream_get_resume_token()` or the ``_id`` field  of any change received from a change stream can be used here.  This option is mutually exclusive with ``resumeAfter`` and ``startAtOperationTime``.'}),
        ('startAtOperationTime', {'type': 'timestamp', 'help': 'A ``Timestamp``. The change stream only provides changes that occurred at or after the specified timestamp. Any command run against the server will return an operation time that can be used here. This option is mutually exclusive with ``resumeAfter`` and ``startAfter``.'}),
        ('maxAwaitTimeMS', {'type': 'int64_t', 'convert': '_mongoc_convert_int64_positive', 'help': 'An ``int64`` representing the maximum amount of time a call to :symbol:`mongoc_change_stream_next` will block waiting for data'}),
        ('fullDocument', {
            'type': 'utf8',
            'help': 'An optional UTF-8 string. Set this option to "default", '
                    '"updateLookup", "whenAvailable", or "required", If unset, '
                    'The string "default" is assumed. Set this option to '
                    '"updateLookup" to direct the change stream cursor to '
                    'lookup the most current majority-committed version of the '
                    'document associated to an update change stream event.'
        }),
        ('fullDocumentBeforeChange', {
            'type': 'utf8',
            'help': 'An optional UTF-8 string. Set this option to '
                    '"whenAvailable", "required", or "off". When unset, the '
                    'default value is "off". Similar to "fullDocument", but '
                    'returns the value of the document before the associated '
                    'change.',
        }),
        ('showExpandedEvents', { 'type': 'bool', 'help': 'Set to ``true`` to return an expanded list of change stream events. Available only on MongoDB versions >=6.0'}),
        comment_option_string_pre_4_4,
    ], fullDocument=None, fullDocumentBeforeChange=None)),

    ('mongoc_create_index_opts_t', Struct([
        write_concern_option,
        session_option,
    ], opts_name='command_opts')),

    ('mongoc_read_write_opts_t', Struct([
        read_concern_document_option,
        write_concern_option,
        session_option,
        collation_option,
        server_option,
    ])),

    # Only for documentation - we use mongoc_read_write_opts_t for real parsing.
    ('mongoc_read_opts_t', Struct([
        read_concern_document_option,
        session_option,
        collation_option,
        server_option,
    ], generate_code=False)),

    ('mongoc_write_opts_t', Struct([
        write_concern_option,
        session_option,
        collation_option,
        server_option,
    ], generate_code=False)),

    ('mongoc_gridfs_bucket_opts_t', Struct([
        ('bucketName', {'type': 'utf8', 'help': 'A UTF-8 string used as the prefix to the GridFS "chunks" and "files" collections. Defaults to "fs". The bucket name, together with the database and suffix collections must not exceed 120 characters. See the manual for `the max namespace length <https://www.mongodb.com/docs/manual/reference/limits/#Namespace-Length>`_.'}),
        ('chunkSizeBytes', {'type': 'int32_t', 'convert': '_mongoc_convert_int32_positive', 'help': 'An ``int32`` representing the chunk size. Defaults to 255KB.'}),
        write_concern_option,
        read_concern_option
    ], bucketName="fs", chunkSizeBytes=(255 * 1024))),

    ('mongoc_gridfs_bucket_upload_opts_t', Struct([
        ('chunkSizeBytes', {'type': 'int32_t', 'convert': '_mongoc_convert_int32_positive', 'help': 'An ``int32`` chunk size to use for this file. Overrides the ``chunkSizeBytes`` set on ``bucket``.'}),
        ('metadata', {'type': 'document', 'help': 'A :symbol:`bson_t` representing metadata to include with the file.'})
    ])),

    ('mongoc_aggregate_opts_t', Struct([
        read_concern_option,
        write_concern_option,
        session_option,
        bypass_option,
        collation_option,
        server_option,
        ('batchSize', {'type': 'int32_t', 'help': 'An ``int32`` representing number of documents requested to be returned on each call to :symbol:`mongoc_cursor_next`', 'check_set': True}),
        let_option,
        comment_option_string_pre_4_4,
        hint_option,
    ])),

    ('mongoc_find_and_modify_appended_opts_t', Struct([
        write_concern_option,
        session_option,
        hint_option,
        let_option,
        comment_option_since_4_4,
    ], opts_name='extra')),

    ('mongoc_count_document_opts_t', Struct([
        read_concern_document_option,
        session_option,
        collation_option,
        server_option,
        # The CRUD spec specifies `skip` and `limit` as int64_t. The server appears to accept int32 and double. The C driver accepts any bson_value_t and relies on the server to return an error for an invalid type.
        ('skip', {'type': 'bson_value_t', 'help': 'An int specifying how many documents matching the ``query`` should be skipped before counting.'}),
        ('limit', {'type': 'bson_value_t', 'help': 'An int specifying the maximum number of documents to count.'}),
        comment_option_string_pre_4_4,
        hint_option
    ])),

])

header_comment = """/**************************************************
 *
 * Generated by build/%s.
 *
 * DO NOT EDIT THIS FILE.
 *
 *************************************************/
/* clang-format off */""" % basename(__file__)


def paths(struct):
    """Sequence of path, option name, option info."""
    for option_name, info in struct.items():
        the_type = info['type']
        the_field = info.get('field', option_name)
        if the_type in opts_structs:
            # E.g., the type is mongoc_crud_opts_t. Recurse.
            sub_struct = opts_structs[the_type]
            for path, sub_option_name, sub_info in paths(sub_struct):
                yield ('%s.%s' % (the_field, path),
                       sub_option_name,
                       sub_info)
        else:
            yield the_field, option_name, info


def path_to(the_type, the_field):
    """Like "mongoc_update_one_opts->update.crud.write_concern_owned"."""
    for path, name, info in paths(opts_structs[the_type]):
        if name == the_field:
            return path

    raise ValueError(
        "No field '%s' in '%s'" % (the_field, the_type))


env = Environment(loader=FileSystemLoader(template_dir),
                  trim_blocks=True,
                  extensions=['jinja2.ext.loopcontrols'])

files = ["mongoc-opts-private.h", "mongoc-opts.c"]

for file_name in files:
    print(file_name)
    with open(joinpath(src_dir, file_name), 'w+') as f:
        t = env.get_template(file_name + ".template")
        f.write(t.render(globals()))
        f.write('\n')


def document_opts(struct, f):
    for option_name, info in struct.items():
        if info.get('internal') or info.get('hidden'):
            continue

        the_type = info['type']
        if the_type in opts_structs:
            # E.g., the type is mongoc_crud_opts_t. Recurse.
            document_opts(opts_structs[the_type], f)
            continue

        assert 'help' in info, "No 'help' for '%s'" % option_name
        f.write("* ``{option_name}``: {info[help]}\n".format(**locals()))


disclaimer = """
..
   Generated with build/generate-opts.py
   DO NOT EDIT THIS FILE

"""
for struct_name, struct in opts_structs.items():
    if not struct.generate_rst:
        continue

    name = re.sub(r'mongoc_(\w+)_t', r'\1', struct_name).replace('_', '-')
    file_name = name + '.txt'
    print(file_name)
    f = open(joinpath(doc_includes, file_name), 'w')
    f.write (disclaimer)
    f.write(
        "``%s`` may be NULL or a BSON document with additional"
        " command options:\n\n" % struct.opts_name)
    document_opts(struct, f)

    f.close()
