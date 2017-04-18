mkdir vide

for file in *
do
if [ -s ${file} ]
then
	mv ${file} ./nonVide/${file}
else
	mv ${file} ./vide/${file}
fi
done

