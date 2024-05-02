/usr/lib/spark/bin/spark-submit --master local --deploy-mode client /lessons/dags/project_sp7_users.py /user/damirkalin/data/geo/events/event_type=message 2022-06-21 28 /user/damirkalin/data/geo/geo.csv /user/damirkalin/data/geo/timezone.csv /user/damirkalin/analytics/project_sp7_users_d28


/usr/lib/spark/bin/spark-submit --master local --deploy-mode client /lessons/dags/project_sp7_zone.py /user/damirkalin/data/geo/events 2022-02-28 7 /user/damirkalin/data/geo/geo.csv /user/damirkalin/analytics/project_sp7_zone_d28

/usr/lib/spark/bin/spark-submit --master local --deploy-mode client /lessons/dags/project_sp7_recomendation.py /user/damirkalin/data/geo/events 2022-02-28 30 /user/damirkalin/data/geo/geo.csv /user/damirkalin/data/geo/timezone.csv /user/damirkalin/analytics/project_sp7_recomendation_d30