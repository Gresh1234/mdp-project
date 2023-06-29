# mdp-project
Proyecto de Procesamiento Masivo de Datos CC-5212, Grupo 15

## Cómo ejecutar

1. Descargar el dataset desde [Kaggle](https://www.kaggle.com/datasets/rsrishav/youtube-trending-video-dataset/versions/1047) (esta fue la versión del dataset utilizada en el proyecto, pero se actualiza a diario).
2. Colocar los .csv en la carpeta data
3. Ejecutar el notebook `youtube-trending-video-dataset-cleaning.ipynb` para generar el archivo .tsv.
4. Compilar el .jar con Ant.
5. Copiar `data/youtube_kaggle_sample_dataset.tsv`, [nombre del script de pig], y `dist/mdp-project-burstdetector.jar` al master del cluster.
6. Copiar el archivo `youtube_kaggle_sample_dataset.tsv` a HDFS.
