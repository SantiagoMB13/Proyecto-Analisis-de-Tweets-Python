# Proyecto-Análisis-Tweets-Python-Paralelizado
El archivo se probó en Docker con la imagen modificada que le mandé por correo y funcionó correctamente.

El filtro de fecha_inicial y fecha_final es inclusivo, es decir que si por ejemplo fi es 01/01/2016, se incluiran los tweets del 01/01/2016 en adelante.

Para los hashtags puedes usar o no el #, es decir, tanto #DmMeCarter como DmMeCarter son validos en una línea del archivo de hashtags.

Se consiguió reducir el tiempo de ejecución a algo menos de la mitad. La mayor eficiencia se consigue con 6 nodos (-n 6) y la peor con 2 nodos. Sin embargo, la diferencia en tiempo no varía demasiado cambiando la cantidad de nodos.
