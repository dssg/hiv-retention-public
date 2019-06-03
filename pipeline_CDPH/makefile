labels :
	sh sql/make_cdph_labels.sh

FEATURES_DIR = ./sql/features/FEATURE*
features : $(FEATURES_DIR)
	for file in $^ ; do \
		psql -f $${file} ; \
	done
