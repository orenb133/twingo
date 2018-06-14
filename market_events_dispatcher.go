package twingo

import (
    "github.com/op/go-logging"
)

//=====================================================================================================================
type MarketDataEventsDispatcher struct {
	logger *logging.Logger
	ingressCh chan *MarketDataMessage
	egressTradeChs []chan <- *MarketTimedEvent
}

//---------------------------------------------------------------------------------------------------------------------
func NewMarketDataEventsDispatcher(aLogger *logging.Logger) *MarketDataEventsDispatcher {
	instance := new(MarketDataEventsDispatcher)
	instance.logger = aLogger
	instance.ingressCh = make(chan *MarketDataMessage, 1)
	
	return instance
}

//---------------------------------------------------------------------------------------------------------------------
func (this *MarketDataEventsDispatcher) InitRegisterEgressTradeChannel(aEgressTradeCh chan <- *MarketTimedEvent) {
	this.egressTradeChs = append(this.egressTradeChs, aEgressTradeCh)
}

//---------------------------------------------------------------------------------------------------------------------
func (this *MarketDataEventsDispatcher) GetIngressChannel() chan <- *MarketDataMessage {
	return this.ingressCh
}

//---------------------------------------------------------------------------------------------------------------------
func (this *MarketDataEventsDispatcher) Dispatch() {
	this.logger.Info("Ready")

	for {
		marketDataMessage, isOpened := <- this.ingressCh
	
	    if !isOpened {
	    	this.logger.Info("Ingress channel closed, done")
	    	
	    	return
		}
			
		this.logger.Debugf("Received market data: %v", marketDataMessage)
		
	    if marketDataMessage.Type == MarketDataMessageTypeUpdate {

    		for index := range marketDataMessage.Events {
				event := &marketDataMessage.Events[index]

    			switch event.Type {

    				case MarketEventTypeTrade:
	    				
	    				for _, ch := range this.egressTradeChs {
	    					timedEvent := new(MarketTimedEvent)
	    					timedEvent.MarketEvent = event
	    					timedEvent.TimestampMsec = marketDataMessage.TimestampMsec
	    					
	    					this.logger.Debugf("Dispatching timed market trade event: %v", timedEvent)
	    					ch <- timedEvent
	    				}

    				default:
    				
	    				this.logger.Debugf("Ignoring event: %v", event)
    			}
			}
	    } 
	}
}
