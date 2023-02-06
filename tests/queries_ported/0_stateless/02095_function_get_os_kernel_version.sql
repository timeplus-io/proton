WITH split_by_char(' ', get_os_kernel_version()) AS version_pair SELECT version_pair[1]
