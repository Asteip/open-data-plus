clear
echo ------------------------
#!/bin/bash

if [ $# = 1 ]; then

	echo "mvn package"
	mvn package

	echo "------------------------"

	case $1 in 
		1) 
			if [ -d "resultsPhase1" ]; then
				echo "------------------------"
				echo "suppression resultsPhase1"
				rm -r resultsPhase1
				echo "------------------------"
			fi

			echo "PHASE 1"
			nohup spark-submit --class com.alma.opendata.NQuadsSearch --driver-memory 100G target/open-data-crawler-1.0-SNAPSHOT-jar-with-dependencies.jar 1 ../../CAPSTONE_OPEN_DATA_PLUS/ &
			;;
		2)
			if [ -d "resultsPhase2" ]; then
				echo "------------------------"
				echo "suppression resultsPhase2"
				rm -r resultsPhase2
				echo "------------------------"
			fi

			echo "PHASE 2"
			nohup spark-submit --class com.alma.opendata.NQuadsSearch --driver-memory 100G target/open-data-crawler-1.0-SNAPSHOT-jar-with-dependencies.jar 2 ../../CAPSTONE_OPEN_DATA_PLUS/ ./resultsPhase1/* &
			;;

		3)
			if [ -d "resultsArbre" ]; then
				echo "------------------------"
				echo "suppression resultsArbre"
				rm -r resultsArbre
				echo "------------------------"
			fi

			echo "PHASE 3"
			nohup spark-submit --class com.alma.opendata.NQuadsSearch --driver-memory 100G target/open-data-crawler-1.0-SNAPSHOT-jar-with-dependencies.jar 3 ../../CAPSTONE_OPEN_DATA_PLUS/ ./resultsPhase2/* &
			;;

		*)
			echo "Aucune phase : $1"
			;;
	esac
else
	echo "Utilisation : ./Compile_Run.sh <Numero de phase>"
	echo "Numero de phase : "
	echo "		0 : pour compiler seulement"
	echo "		1 : pour la phase 1"
	echo "		2 : pour la phase 2"
	echo "		3 : pour la phase 3"
fi
