#!/bin/bash

KAFKA_PATH=~/kafka_2.13-3.5.0
TOPIC_NAME=mta_turnstile_topic
BROKER=localhost:9092

# Correct format: C/A, Unit, SCP, Station, Line, Division
stations=(
    "R023,R045,00-00-00,34 ST-HERALD SQ,BDFMNQRW,BMT"
    "B013,R081,02-00-00,161/YANKEE STAD,BDX4,IND"
    "A001,R079,01-00-00,WORLD TRADE CTR,E,IND"
    "R016,R032,00-00-01,TIME SQ-42 ST,1237ACENQRSWZ,IRT"
    "A002,R051,02-00-00,34 ST-PENN STA,ACE123,IRT/BMT"
    "A002,R170,00-00-00,FULTON ST,2345ACJZ,IRT/BMT"
    "A004,R033,00-00-00,125 ST,456,IRT"
    "R029,R044,00-00-00,23 ST,123FM,IRT/BMT"
    "R022,R033,01-00-00,42 ST-PORT AUTH,ACE123NRQW,IND"
    "R028,R088,00-00-00,CANAL ST,JZNQ6,BMT/IRT"
)

# Weights for each station (higher weight = more frequent)
weights=(20 18 15 12 10 8 6 5 4 2)

pick_station() {
    local total=0
    for w in "${weights[@]}"; do
        total=$((total + w))
    done

    local rand=$((RANDOM % total))
    local idx=0

    for i in "${!weights[@]}"; do
        if (( rand < weights[i] )); then
            idx=$i
            break
        fi
        rand=$((rand - weights[i]))
    done

    echo "${stations[$idx]}"
}

while true
do
  TIMESTAMP=$(date +"%m/%d/%Y,%H:%M:%S")
  HOUR=$(date +"%H")
  station_info=$(pick_station)

  IFS=',' read -r CA UNIT SCP STATION LINE DIVISION <<< "$station_info"

  # Rush hour logic
  if [ "$HOUR" -ge 7 ] && [ "$HOUR" -lt 10 ]; then
   ENTRIES=$((RANDOM % 40 + 10))  # 10–49
   EXITS=$((RANDOM % 18 + 2))     # 2–19
  elif [ "$HOUR" -ge 17 ] && [ "$HOUR" -lt 20 ]; then
   ENTRIES=$((RANDOM % 18 + 2))   # 2–19
   EXITS=$((RANDOM % 40 + 10))    # 10–49
  else
   ENTRIES=$((RANDOM % 12 + 3))   # 3–14
   EXITS=$((RANDOM % 12 + 3))     # 3–14
  fi
  
  # Weekend scaling
  DAY=$(date +"%u")  # 1 = Monday, ..., 7 = Sunday
  if [ "$DAY" -eq 6 ] || [ "$DAY" -eq 7 ]; then
    ENTRIES=$((ENTRIES / 2))
    EXITS=$((EXITS / 2))
  fi

  # Holiday scaling
  HOLIDAYS=("01/01" "07/04" "12/25")
  TODAY=$(date +"%m/%d")
  for h in "${HOLIDAYS[@]}"; do
    if [ "$TODAY" == "$h" ]; then
      ENTRIES=$((ENTRIES / 3))
      EXITS=$((EXITS / 3))
      break
    fi
  done

  MESSAGE="$CA,$UNIT,$SCP,$STATION,$LINE,$DIVISION,$TIMESTAMP,REGULAR,$ENTRIES,$EXITS"

  echo "$MESSAGE" | $KAFKA_PATH/bin/kafka-console-producer.sh --broker-list $BROKER --topic $TOPIC_NAME > /dev/null

  echo " Sent: $MESSAGE"

  sleep $((RANDOM % 3 + 3))  # 3–5 sec


done

