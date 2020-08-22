# Go Kit NATS Zipkin Tracing
This package implements native zipkin tracing for NATS transport (Subscriber and Publisher) of Go Kit toolkit.

###Context transfer
This package transfers the span context inside of a NATS message as a separate field `natsSpanContextB3Map`. **b3.Map**'s `Inject`, `Extract` methods handle Serialization and deserialization of the context.  
The `nats.Msg.Data` object structure to pass data and the context:
````
type natsMessageWithContext struct {
	Sc   b3.Map `json:"natsSpanContextB3Map"`
	Data []byte `json:"data"`
}
````
Data field holds marshalled `nats.Msg.Data`.

**Make sure** to use `NATSPublisherTrace` endpoints only with enpoints that use `NATSSubscriberTrace` to deserialize messages properly. Or use `AllowPropagation` option to disallow propagation.

 