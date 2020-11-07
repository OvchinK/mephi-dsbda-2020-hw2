month=$(date +%b)
day=$(date +%d)
host="$(hostname)"
process="generator"
message="Something happened!"

function genRow
{
    hour=$(( RANDOM % 24 ))
    minute=$(( RANDOM % 60 ))
    second=$(( RANDOM % 60 ))
    priority=
    case $(( RANDOM % 11 )) in
        0  ) priority="debug";;
        1  ) priority="info";;
        2  ) priority="notice";;
        3  ) priority="warning";;
        4  ) priority="warn";;
        5  ) priority="error";;
        6  ) priority="err";;
        7  ) priority="crit";;
        8  ) priority="alert";;
        9  ) priority="emerg";;
        10 ) priority="panic";;
    esac
    if [ $priority ]
    then
        echo $month $day $hour:$minute:$second "$host" "$process": \<$priority\> "$message"
    else
        echo $month $day $hour:$minute:$second "$host" "$process": "$message"
    fi
}

for i in `seq $1`
do
    genRow
done
