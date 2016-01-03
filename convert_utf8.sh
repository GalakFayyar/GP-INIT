#!/bin/bash

# cd data/

# iconv -f ISO-8859-1 activites.csv -t UTF-8 -o activites_utf8.csv
# rm activites.csv
# mv activites_utf8.csv activites.csv

# iconv -f ISO-8859-1 etablissements_part1.csv -t UTF-8 -o etablissements_part1_utf8.csv
# rm etablissements_part1.csv
# mv etablissements_part1_utf8.csv etablissements_part1.csv

# iconv -f ISO-8859-1 etablissements_part2.csv -t UTF-8 -o etablissements_part2_utf8.csv
# rm etablissements_part2.csv
# mv etablissements_part2_utf8.csv etablissements_part2.csv

# iconv -f ISO-8859-1 activites_connexes.csv -t UTF-8 -o activites_connexes_utf8.csv
# rm activites_connexes.csv
# mv activites_connexes_utf8.csv activites_connexes.csv

# iconv -f ISO-8859-1 referentiel_activites.csv -t UTF-8 -o referentiel_activites_utf8.csv
# rm referentiel_activites.csv
# mv referentiel_activites_utf8.csv referentiel_activites.csv

# iconv -f ISO-8859-1 referentiel_communes.csv -t UTF-8 -o referentiel_communes_utf8.csv
# rm referentiel_communes.csv
# mv referentiel_communes_utf8.csv referentiel_communes.csv

# iconv -f ISO-8859-1 activites_part1.csv -t UTF-8 -o activites_part1_utf8.csv
# rm activites_part1.csv
# mv activites_part1_utf8.csv activites_part1.csv

# iconv -f ISO-8859-1 activites_part2.csv -t UTF-8 -o activites_part2_utf8.csv
# rm activites_part2.csv
# mv activites_part2_utf8.csv activites_part2.csv

# iconv -f ISO-8859-1 activites_part3.csv -t UTF-8 -o activites_part3_utf8.csv
# rm activites_part3.csv
# mv activites_part3_utf8.csv activites_part3.csv

# iconv -f ISO-8859-1 activites_part4.csv -t UTF-8 -o activites_part4_utf8.csv
# rm activites_part4.csv
# mv activites_part4_utf8.csv activites_part4.csv

# iconv -f ISO-8859-1 activites_test.csv -t UTF-8 -o activites_test_utf8.csv
# rm activites_test.csv
# mv activites_test_utf8.csv activites_test.csv

# cd ..

cd data
for file in `*.csv`
	do
		iconv -f ISO-8859-1 $i -t UTF-8 -o $i_utf8.csv
		rm $i.csv
		mv $i_utf8.csv $i.csv
	done
done
cd ..