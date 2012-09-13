#!/bin/bash
# vim :set ft=sh:


get_fs (){
    echo $(ls -lah $1 | cut -f5 -d' ')
}

testfile=testfile
chunk=chunk

echo 'create the test data'
python -c "
import numpy
a = numpy.linspace(0, 100, 2e7)
with open('$testfile', 'w') as test_file:
    test_file.write(a.tostring())
"
echo "$testfile is: $(get_fs $testfile)"

echo -n 'enlarge the testfile'
cat $testfile > $chunk
for i in $( seq 9);
do
    cat $chunk >> $testfile
    echo -n '.'
done
echo ' done.'
rm $chunk

echo "$testfile is: $(get_fs $testfile)"

echo "do compression with bloscpack, chunk-size: 1MB (default)"
/usr/bin/time -p ./blpk --force compress $testfile
echo "$testfile.blp is: $(get_fs $testfile.blp)"

echo "do compression with gzip"
/usr/bin/time -p gzip -c $testfile > $testfile.gz
echo "$testfile.gz is: $(get_fs $testfile.gz)"
