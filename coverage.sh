cover -delete

#find t -name \*.t -print -exec perl -MDevel::Cover {} \;

perl -MDevel::Cover example.pl

perl -MDevel::Cover t/00_compile.t
perl -MDevel::Cover t/94_fixme.t
perl -MDevel::Cover t/95_critic.t
perl -MDevel::Cover t/96_kwalitee.t
perl -MDevel::Cover t/97_distribution.t
perl -MDevel::Cover t/98_pod.t
perl -MDevel::Cover t/99_pod_coverage.t

perl -MDevel::Cover t/00_CappedCollection/00_compile.t
perl -MDevel::Cover t/00_CappedCollection/01_new.t
perl -MDevel::Cover t/00_CappedCollection/02_insert.t
perl -MDevel::Cover t/00_CappedCollection/03_update.t
perl -MDevel::Cover t/00_CappedCollection/04_receive.t
perl -MDevel::Cover t/00_CappedCollection/05_validate.t
perl -MDevel::Cover t/00_CappedCollection/06_pop_oldest.t
perl -MDevel::Cover t/00_CappedCollection/07_exists.t
perl -MDevel::Cover t/00_CappedCollection/08_lists.t
perl -MDevel::Cover t/00_CappedCollection/09_drop.t
perl -MDevel::Cover t/00_CappedCollection/10_quit.t
perl -MDevel::Cover t/00_CappedCollection/11_max_datasize.t
perl -MDevel::Cover t/00_CappedCollection/12_last_errorcode.t
perl -MDevel::Cover t/00_CappedCollection/13_name.t
perl -MDevel::Cover t/00_CappedCollection/14_size.t
perl -MDevel::Cover t/00_CappedCollection/15_size_garbage.t
perl -MDevel::Cover t/00_CappedCollection/98_pod.t
perl -MDevel::Cover t/00_CappedCollection/99_pod_coverage.t

cover


