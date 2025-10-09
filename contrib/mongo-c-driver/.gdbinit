define printbson
	set $bson = $arg0
	if $bson->flags & BSON_FLAG_INLINE
		print ((bson_impl_inline_t*) $bson)->data
		set $data = &(((bson_impl_inline_t*) $bson)->data)
		printf "INLINE"
		__printbson $data
	else
		set $impl = (bson_impl_alloc_t*) $bson
		printf "ALLOC [%p + %d]", $impl->buf, $impl->offset
		__printbson (*$impl->buf) + $impl->data
	end
end

define __printbson
	set $bson = ((uint32_t*)$arg0)
	printf " (len=%d)\n", $bson[0]
	printf "{\n"
	__printElements ($bson+1) 0 1
	printf "\n}\n"
end

define __printElements
	set $data = ((uint8_t*)$arg0)
	set $isDocument = $arg1
	set $depth = $arg2
	set $addComma = 0

	while $data != 0
		set $type = (uint8_t)($data[0])

		if $type == 0
			loop_break
		end

		if $addComma == 1
			printf ",\n"
		end
		set $addComma = 1

		__printSpaces $depth
		printf "'%s' : ", (char*) ($data+1)

		# skip through C String
		while $data[0] != '\0'
			set $data = $data + 1
		end
		set $data = $data + 1

	 	if $type == 0x01
			__printDouble $data
		end
	 	if $type == 0x02
			__printString $data
		end
	 	if $type == 0x03
			__printDocument $data $depth
		end
	 	if $type == 0x04
			__printArray $data $depth
		end
	 	if $type == 0x05
			__printBinary $data
		end
	 	if $type == 0x07
			__printObjectID $data
		end
	 	if $type == 0x08
			__printBool $data
		end
	 	if $type == 0x09
			__printUtcDateTime $data
		end
	 	if $type == 0x0a
			__printNull
		end
	 	if $type == 0x0b
			__printRegex $data
		end
	 	if $type == 0x0d
			__printJavascript $data
		end
	 	if $type == 0x0e
			__printSymbol $data
		end
	 	if $type == 0x0f
			__printJavascriptWScope $data
		end
	 	if $type == 0x10
			__printInt32 $data
		end
	 	if $type == 0x11
			__printTimestamp $data
		end
	 	if $type == 0x12
			__printInt64 $data
		end
	end
end

define __printSpaces
	set $i = 0
	while $i < (4 * $arg0)
		printf " "
		set $i = $i + 1
	end
end


define __printDocument
	set $value = ((uint8_t*) $arg0)
	set $depth = $arg1
	printf "{\n"
	__printElements ($value+4) 1 ($depth+1)
	printf "\n"
	__printSpaces ($depth-1)
	printf "}"
	set $depth = $depth-1
	set $data = $data + 1
end

define __printArray
	set $value = ((uint8_t*) $arg0)
	set $depth = $arg1
	printf "[\n"
	__printElements ($value+4) (0) ($depth+1)
	printf "\n"
	__printSpaces ($depth-1)
	printf "]"
	set $depth = $depth-1
	set $data = $data + 1
end


define __printDouble
	set $value = ((double*) $arg0)
	printf "%f", $value[0]
	set $data = $data + 8
end

define __printString
	set $value = ((char*) $arg0) + 4
	printf "\"%s\"", $value
	set $data = $data + 4 + ((uint32_t*)$data)[0]
end

define __printBinary
	set $value = ((uint8_t*) $arg0)
	set $length = ((int32_t*) $arg0)[0]
	printf "Binary(\"%02X\", \"", $value[4]
	set $i = 4
	while $i < $length 
		printf "%02X", $value[$i+5]
		set $i = $i + 1
	end
	printf "\")"
	set $data = $data + 5 + ((uint32_t*)$data)[0]
end

define __printObjectID
	set $value = ((uint8_t*) $arg0)
	set $i = 0
	printf "ObjectID(\""
	while $i < 12
		printf "%02X", $value[$i]
		set $i = $i + 1
	end
	printf "\")"
	set $data = $data + 12
end

define __printBool
	set $value = ((uint8_t*) $arg0)
	printf "%s", $value[0] ? "true" : "false"
	set $data = $data + 1
end

define __printUtcDateTime
	set $value = ((uint64_t*) $arg0)
	printf "UTCDateTime(%ld)", $value[0]
	set $data = $data + 8
end

define __printNull
	printf "null"
end

define __printRegex
	printf "Regex(\"%s\", \"", (char*) $data

	# skip through C String
	while $data[0] != '\0'
		set $data = $data + 1
	end
	set $data = $data + 1
	
	printf "%s\")", (char*) $data

	# skip through C String
	while $data[0] != '\0'
		set $data = $data + 1
	end
	set $data = $data + 1
end

define __printJavascript
	set $value = ((char*) $arg0) + 4
	printf "JavaScript(\"%s\")", $value
	set $data = $data + 4 + ((uint32_t*)$data)[0]
end

define __printSymbol
	set $value = ((char*) $arg0) + 4
	printf "Symbol(\"%s\")", $value
	set $data = $data + 4 + ((uint32_t*)$data)[0]
end

define __printJavascriptWScope
	set $value = ((char*) $arg0) + 8
	printf "JavaScript(\"%s\") with scope: ", $value
	set $data = $data + 8 + ((uint32_t*)$data)[1]
	__printDocument $data $depth
end

define __printInt32
	set $value = ((uint32_t*) $arg0)
	printf "NumberInt(\"%d\")", $value[0]
	set $data = $data + 4
end

define __printTimestamp
	set $value = ((uint32_t*) $arg0)
	printf "Timestamp(%u, %u)", $value[0], $value[1]
	set $data = $data + 8
end

define __printInt64
	set $value = ((uint64_t*) $arg0)
	printf "NumberLong(\"%ld\")", $value[0]
	set $data = $data + 8
end

