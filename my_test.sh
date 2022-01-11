END=6
## print date five times ##
x=$END 
# echo "" > output.txt
# echo "" > output2.txt
# echo "" > output3.txt
while [ $x -gt 0 ]; 
do 
    # echo "test";
    # echo "start test part1";
    # echo "start test part1" >> output.txt;
    # ./raft_test part1 >> output2.txt;

    echo "start test";
    echo "$x"
    # echo "start test part1" >> output.txt;
    # ./raft_test part1 >> output.txt;

    echo "start grade";
    ./grade.sh >> output.txt;

    # echo "start test part3";
    # echo "start test part3" >> output3.txt;
    # ./raft_test part3 >> output3.txt;

    # echo "start test part1" >> output.txt;
    # ./raft_test part1 >> output.txt;
    x=$(($x-1));
done
echo "done"