pip install -r requirements.txt -r test_requirements.txt
./test.sh
nosetests test/test_file_io.py:pack_unpack_hard
