-- Tags: no-fasttest

select filesystem_capacity('s3_disk') >= filesystem_available('s3_disk') and filesystem_available('s3_disk') >= filesystem_unreserved('s3_disk');
select filesystem_capacity('default') >= filesystem_available('default') and filesystem_available('default') >= 0 and filesystem_unreserved('default') >= 0;

select filesystem_capacity('__un_exists_disk'); -- { serverError UNKNOWN_DISK }
