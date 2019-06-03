labels :
	sh sql/make_ucm_labels.sh

helper_DIR = ./sql/features/helper*
helper : $(helper_DIR)
	for file in $^ ; do \
		psql -f $${file} ; \
	done

FEATURES_DIR = ./sql/features/FEATURE*
features_queries : $(FEATURES_DIR)
	for file in $^ ; do \
		psql -f $${file} ; \
	done

features :
	make helper
	make features_queries
