#!/usr/bin/env bash

echo "**************************************************************************"
echo "* searching for last complete build"
echo "**************************************************************************"

wget -q -o /dev/null index.html http://hasky.askmonty.org/archive/10.3/
grep -o ">build-[0-9]*" index.html | grep -o "[0-9]*" | tac | while read -r line ; do

  curl -s --head http://hasky.askmonty.org/archive/10.3/build-$line/kvm-deb-jessie-amd64/md5sums.txt | head -n 1 | grep "HTTP/1.[01] [23].." > /dev/null
  if [ $? = "0" ]; then
    echo "**************************************************************************"
    echo "* Processing $line"
    echo "**************************************************************************"
    wget -q -o /dev/null -O $line.html  http://hasky.askmonty.org/archive/10.3/build-$line/kvm-deb-jessie-amd64/debs/binary/
    grep -o ">[^\"]*\.deb" $line.html | grep -o "[^>]*\.deb" | while read -r file ; do
        if  [[ "$file" =~ ^mariadb-plugin.* ]] ;
        then
          echo "skipped file: $file"
        else
          echo "download file: $file"
          wget -q -o /dev/null -O .travis/build/$file http://hasky.askmonty.org/archive/10.3/build-$line/kvm-deb-jessie-amd64/debs/binary/$file
        fi
    done

    exit
  else
    echo "skip build $line"
  fi
done



