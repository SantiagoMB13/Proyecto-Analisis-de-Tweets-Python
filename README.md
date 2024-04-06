# Proyecto-Análisis-Tweets-Python
Basado en una base de datos de tweets donde los tweets se encuentran en formato JSON dentro de un archivo comprimido, se utiliza un script de python para hacer jsons y grafos de los retweets, menciones y coretweets. Este script se despliega en Docker.

El programa acepta los siguientes parámetros de entrada:

  a. -d <path relativo> : (valor por defecto, data) Directorio relativo en donde podrá encontrar los tweets. Los mismos pueden estar en subdirectorios, el generador debe recorrer toda la estructura de la     carpeta para buscarlos.

  b. -fi <fecha inicial> : fecha (dd-mm-aa) a partir de la cual se deben tomar en cuenta los tweets. Ignorar la restricción en caso de que el parámetro no esté presente

  c. -ff <fecha final> : fecha (dd-mm-aa) hasta la cual se deben tomar en cuenta los tweets. Ignorar la restricción en caso de que el parámetro no esté presente

  d. -h <nombre de archivo>: Nombre de archivo de texto en el que se encuentra los hashtags por los cuales se filtrarán los tweets, uno en cada línea. Ignorar la restricción en caso de que el parámetro no    esté presente

Las salidas dependen de los siguientes parámetros:

  a. -grt: Grafo de retweets (llamado rt.gexf), todos los nodos

  b. -jrt: JSON de retweets (llamado rt.json) en el que se presentan los autores con cada uno de sus tweets (id) que tuvieron retweet, y una lista de los usuarios que le hicieron retweet a cada uno.     
  Ordenar el JSON de mayor a menor por número total de retweets al usuario

  c. -gm: Grafo de menciones (mención.gexf)

  d. -jm: JSON de menciones (mención.json) en el que se presentan los autores que tuvieron mención y una lista de los usuarios que lo mencionaron con el id del tweet de la mención. Ordenar el JSON de mayor   a menor por número total de menciones al usuario

  e. -gcrt: Grafo corretweets (corrtw.gexf) - Basado en "The Co-Retweeted Network and its Applications for Measuring the Perceived Political Polarization"

  f. -jcrt: JSON de corretweets (corrtw.json) en el que se presenta cada par de autores y una lista de los autores que les hicieron retweet.

El filtro de fecha_inicial y fecha_final es inclusive, es decir que si por ejemplo fi es 01/01/2016, se incluiran los tweets del 01/01/2016 en adelante.

Para los hashtags puedes usar o no el #, es decir, tanto #DmMeCarter como DmMeCarter son validos en una línea del archivo de hashtags.

generador.py es el archivo secuencial y generadorp.py es el archivo paralelizado.

Se consiguió reducir el tiempo de ejecución a algo menos de la mitad. La mayor eficiencia se consigue con 6 nodos (-n 6) y la peor con 2 nodos. Sin embargo, la diferencia en tiempo no varía demasiado cambiando la cantidad de nodos.
