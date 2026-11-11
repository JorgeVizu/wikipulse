¿Qué tienen en común los artículos de Julio Maldonado, Virtus y Jordi Wild? Encabezaron el ranking de ediciones de Wikipedia en español entre las 13:24:30 y las 13:29:30 UTC del 11 de noviembre. Julio Maldonado y Virtus empataron a tres ediciones. Jordi Wild quedó tercero con dos.

Ese mismo día se publicó que Santiago Abascal había cancelado su entrevista en The Wild Project. El historial de Wikipedia muestra que la reversión de las 13:40 deshizo las dos ediciones de Jordi Wild registradas en el ranking y restauró dos campos de la ficha biográfica. Esa reversión ocurrió después de la ventana de la captura. WikiPulse me mostró cuándo se editó el artículo y el historial me permitió comprobar qué se había cambiado.

Construí WikiPulse con los cambios públicos de Wikimedia. Python los recibe en directo y Kafka conserva el flujo para que Flink pueda retomar la lectura. Flink cuenta ediciones distintas por artículo usando su hora original y publica un top 10 cada 30 segundos cuando el watermark permite cerrar la ventana.

Reinicié el TaskManager con la ingesta en directo. Flink restauró el checkpoint 138 y el panel pasó de la ventana que cerró a las 13:45:30 UTC a la que cerró a las 13:46:00 UTC. En el repositorio dejé también una prueba con datos controlados para comprobar los conteos tras un reinicio, una captura real para reproducir la demo y un modo opcional de ingesta con Azure Event Hubs.

Código y pasos de la demo → https://github.com/JorgeVizu/wikipulse

#DataEngineering #ApacheKafka #ApacheFlink #AzureEventHubs
