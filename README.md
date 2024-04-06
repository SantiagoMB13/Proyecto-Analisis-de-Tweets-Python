# Proyecto-Análisis-Tweets-Python
Basado en una base de datos de tweets, se utiliza un script de python para hacer jsons y grafos de los retweets, menciones y coretweets. Este script se despliega en Docker.

El filtro de fecha_inicial y fecha_final es inclusivo, es decir que si por ejemplo fi es 01/01/2016, se incluiran los tweets del 01/01/2016 en adelante.

Para los hashtags puedes usar o no el #, es decir, tanto #DmMeCarter como DmMeCarter son validos en una línea del archivo de hashtags.

generador.py es el archivo secuencial y generadorp.py es el archivo paralelizado.

Se consiguió reducir el tiempo de ejecución a algo menos de la mitad. La mayor eficiencia se consigue con 6 nodos (-n 6) y la peor con 2 nodos. Sin embargo, la diferencia en tiempo no varía demasiado cambiando la cantidad de nodos.
