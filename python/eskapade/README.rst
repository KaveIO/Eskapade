Dependencies
------------

- analysis depends on core
- core contains no links and depends on no other subpackage
- core_ops depends on core
- data_quality depends on core, analysis, root_analysis, spark_analysis and simulation
- databases depends on core
- etl depends on core, spark_analysis, databases and machine_learning (to do: remove machine_learning dependency. See
mongodftocollection link)
- feedback_loop depends on core and root_analysis
- machine_learning depends on core and databases (and ROOT and pyspark)
- restapi depends on core, machine_learning, databases and etl
- root_analysis depends on core and analysis
- simulation depends on core and spark_analysis
- spark_analysis depends on core and analysis
- text depends on core and machine_learning
- visualization depends on core
