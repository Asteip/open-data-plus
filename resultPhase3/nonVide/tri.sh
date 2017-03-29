for file in *
do
if [ -s ${file} ]
then
	mv ${file} ./nonVide/${file}
fi
done

