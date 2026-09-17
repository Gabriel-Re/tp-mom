# TP MOM - Gabriel Re (105095)

## Decisiones

### BrokerClient

Separé `QueueMiddleware` y `ExchangeMiddleware` porque tienen distinta forma de distribuir los mensajes. La conexión, el envío, el consumo y el cierre quedaron en `BrokerClient` para no repetir lógica. Cada middleware tiene su propia conexión y canal.

Las colas no son durables y no agregué reconexión automática.

### Queue

Para queue uso una cola compartida por nombre. Publico por el exchange predeterminado, usando el nombre de la cola como routing key. Los consumidores se reparten los mensajes.

### Exchange

Para exchange uso `direct` porque el envío depende de keys exactas. Al empezar a consumir creo una cola privada y la vinculo a las keys del suscriptor. Así, dos suscriptores de la misma key reciben cada uno su copia. La cola se borra al cancelar el consumidor o cerrar su conexión.

`Send` publica una vez por key, si una publicación falla, las anteriores pueden haberse enviado.

### Confirmaciones y QoS

Uso ACK manual después de procesar el mensaje y NACK con reencolado. Solo tomo la primera confirmación de cada entrega. Configuro QoS en 1 para no acumular mensajes sin confirmar en un consumidor.

### Concurrencia y cierre

Protejo el estado con un mutex. El callback se ejecuta sin el mutex tomado y debe llamar a ACK o NACK antes de retornar, en la misma goroutine.

`StopConsuming` cancela la suscripción, pero no interrumpe un callback en ejecución. Para reiniciar espero que termine el `StartConsuming` anterior. `Close` libera canal y conexión, y se puede llamar más de una vez.

### Errores y recursos

Distingo una desconexión de un cierre solicitado y devuelvo los errores de la interfaz. Si falla el armado de una suscripción, limpio la cola privada que se haya creado.


