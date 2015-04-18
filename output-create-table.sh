MYSQLCMD="show create table "
ACTIVITIES="$MYSQLCMD Activities;"
ASSAY2GENE="$MYSQLCMD Assay2Gene;"
ASSAYS="$MYSQLCMD Assays;"
SUBSTANCES="$MYSQLCMD Substances;"
UPDATELOG="$MYSQLCMD UpdateLog;"
mysql -u"kharland" -p"Aldous@1" ccb -s -r -e"$ACTIVITIES $ASSAY2GENE $ASSAYS $SUBSTANCES $UPDATELOG"

