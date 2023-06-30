# mdp-project
Proyecto de Procesamiento Masivo de Datos CC-5212, Grupo 15

## Cómo ejecutar

1. Descargar el dataset desde [Kaggle](https://www.kaggle.com/datasets/rsrishav/youtube-trending-video-dataset/versions/1047) (esta fue la versión del dataset utilizada en el proyecto, pero se actualiza a diario).
2. Colocar los .csv en la carpeta data
3. Ejecutar el notebook `youtube-trending-video-dataset-cleaning.ipynb` para generar el archivo .tsv.
4. Compilar el .jar con Ant.
5. Copiar `data/youtube_kaggle_sample_dataset.tsv`, `keywordsCount.pig`, y `dist/mdp-project-burstdetector.jar` al master del cluster.
6. Copiar el archivo `youtube_kaggle_sample_dataset.tsv` a HDFS.
7. Ejecutar el script de Pig con `pig keywords-count.pig`
8. Se puede revisar el output del script con `hdfs dfs -cat /uhadoop2023/proyects/lostilines/output/part-r-00000 | more`.
9. Crear dos topics para el stream de YouTube y el stream filtrado con `kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic groupname -example` (en el proyecto se utilizaron `lostilines-youtube` y `lostilines-youtube-filtered`, respectivamente).
10. Ejecutar el detector de bursts con `java -jar mdp-project-burstdetector.jar BurstDetector lostilines-youtube-filtered [FIFO_SIZE] [EVENT_START_TIME_INTERVAL]` (aquí se utilizó `FIFO_SIZE = 10` y `EVENT_START_TIME_INTERVAL = 6`, que se mide en horas).
11. Ejecutar el filtro de keywords con `java -jar mdp-project-burstdetector.jar VideoFilter lostilines-youtube lostilines-youtube-filtered [keyword1] [keyword2] ...` (las keywords utilizadas se detallan en la presentación, pero están contenidas en los resultados del script de Pig).
12. Ejecutar el simulador de YouTube con `java -jar mdp-project-burstdetector.jar YouTubeSimulator youtube_kaggle_sample_dataset.tsv lostilines-youtube [SPEEDUP]` (se utilizó `SPEEDUP = 10000000`)
