package twingo

import (
	"fmt"
    "golang.org/x/net/websocket"
    "github.com/op/go-logging"
)

//=====================================================================================================================
type MarketDataListener struct {
	logger *logging.Logger
	egressCh chan <- *MarketDataMessage
	webSocket *websocket.Conn
	stopCh chan int
	isListenning bool
	baseUrl string
	symbol MarketDataSymbol
}

//---------------------------------------------------------------------------------------------------------------------
func NewMarketDataListener(aLogger *logging.Logger) *MarketDataListener{
	instance := new(MarketDataListener)
	instance.logger = aLogger
	
	return instance
}

//---------------------------------------------------------------------------------------------------------------------
func (this *MarketDataListener) InitBaseUrl(aBaseUrl string) {
	this.baseUrl = aBaseUrl
}

//---------------------------------------------------------------------------------------------------------------------
func (this *MarketDataListener) InitSymbol(aSymbol MarketDataSymbol) {
	this.symbol = aSymbol
}

//---------------------------------------------------------------------------------------------------------------------
func (this *MarketDataListener) InitEgressChannel(aEgressCh chan <- *MarketDataMessage) {
	this.egressCh = aEgressCh
}

//---------------------------------------------------------------------------------------------------------------------
func (this *MarketDataListener) Connect() error {
	var err error
	url := this.baseUrl + "/" + string(this.symbol)
	this.logger.Infof("Connecting to market data websocket %s", url)
	this.webSocket, err = websocket.Dial(url, "", "http://localhost")
	
	 if err != nil {
	 	 this.logger.Errorf("Failed connecting URL: %s", err)
	 	 
	     return err
	 }
	 
	 return nil
}

//---------------------------------------------------------------------------------------------------------------------
func (this *MarketDataListener) Listen() {
	this.isListenning = true
	var err error
	this.logger.Info("Ready")
	
	for {
		data := new(MarketDataMessage);
    	err = websocket.JSON.Receive(this.webSocket, data)
    	
        if err != nil {
            this.logger.Errorf("Failed receiving market data: %s", err)
			this.isListenning = false
			
            return
        }

		this.logger.Debugf("Received new market data: %v", data)		        
	    this.egressCh <- data
	    
	    select {
	
	    	case <- this.stopCh:
	    		this.isListenning = false
	    		this.logger.Infof("Stopped listenning to market data: %s", this.symbol)
	    		
	    		return
	
	    	default:
	    }
	}
}

//---------------------------------------------------------------------------------------------------------------------
func (this *MarketDataListener) Disconnect() {
	if this.isListenning {
		this.logger.Info("Disconnecting from market data websocket")
		
		this.stopCh <- 0
	}
}

//=====================================================================================================================
type MarketDataSymbol string
const (  
	MarketDataSymbolBtcUsd MarketDataSymbol = "btcusd"
    MarketDataSymbolEthUsd MarketDataSymbol = "ethusd"
    MarketDataSymbolEthBtc MarketDataSymbol = "ethbtc"
)

//=====================================================================================================================
type MarketDataMessage struct {
	Type MarketDataMessageType `json:"type"`
	Sequence int64 `json:"socket_sequence"`
	Id int64 `json:"eventId"`
	TimestampMsec int64 `json:"timestampms"`
	Events []MarketEvent `json:"events"`
}

//=====================================================================================================================
type MarketDataMessageType int64
const (  
	MarketDataMessageTypeHeartbeat MarketDataMessageType  = 0
    MarketDataMessageTypeUpdate MarketDataMessageType = 1
)

//---------------------------------------------------------------------------------------------------------------------
func (this *MarketDataMessageType) UnmarshalJSON(aBytes []byte) error {
	if len(aBytes) > 1 {

		if aBytes[1] == uint8('u') {
			*this = MarketDataMessageTypeUpdate

			return  nil
	
		} else if aBytes[1] == uint8('h') {
			*this = MarketDataMessageTypeHeartbeat

			return  nil
	
		} else {
			return fmt.Errorf("Unknown market data type %b", aBytes)
		}
	}
	
	return fmt.Errorf("Empty market data type %b", aBytes)
}

//=====================================================================================================================
type MarketEvent struct {
	Type MarketEventType `json:"type"`
	Price float64 `json:"price,string"`
}

//=====================================================================================================================
type MarketTimedEvent struct {
	MarketEvent *MarketEvent
	TimestampMsec int64
}

//=====================================================================================================================
type MarketEventType int64
const (  
	MarketEventTypeChange MarketEventType = 0
    MarketEventTypeTrade MarketEventType = 1
    MarketEventTypeAuction MarketEventType = 2
)

//---------------------------------------------------------------------------------------------------------------------
func (this *MarketEventType) UnmarshalJSON(aBytes []byte) error {
	if len(aBytes) > 1 {

		if aBytes[1] == uint8('c') {
			*this = MarketEventTypeChange
		
			return  nil
		
		} else if aBytes[1] == uint8('t') {
			*this = MarketEventTypeTrade
		
			return  nil
			
		
		} else if aBytes[1] == uint8('a') {
			*this = MarketEventTypeAuction
		
			return  nil
		
		} else {
		
			return fmt.Errorf("Unknown market event type %b", aBytes)
		}
	}
	
	return fmt.Errorf("Empty market event type %b", aBytes)
}