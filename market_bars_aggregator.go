package twingo

import (
	"math"
	"time"
	"github.com/op/go-logging"
)

//=====================================================================================================================
type MarketBarsAggregator struct {
	logger *logging.Logger
	ingressCh chan *MarketTimedEvent
	egressCh chan <- *MarketBar
	barDuration time.Duration
}

//---------------------------------------------------------------------------------------------------------------------
func NewMarketBarsAggregator(aLogger *logging.Logger) *MarketBarsAggregator{
	instance := new(MarketBarsAggregator)
	instance.logger = aLogger
	instance.ingressCh = make(chan *MarketTimedEvent, 1)
	
	return instance
}

//---------------------------------------------------------------------------------------------------------------------
func (this *MarketBarsAggregator) InitBarDuration(aBarDuration time.Duration) {
	this.barDuration = aBarDuration
} 

//---------------------------------------------------------------------------------------------------------------------
func (this *MarketBarsAggregator) InitEgressChannel(aEgressCh chan <- *MarketBar) {
	this.egressCh = aEgressCh
}

//---------------------------------------------------------------------------------------------------------------------
func (this *MarketBarsAggregator) GetIngressChannel() chan <- *MarketTimedEvent {
	return this.ingressCh
}

//---------------------------------------------------------------------------------------------------------------------
func (this *MarketBarsAggregator) Aggregate() {
	var currentMarketBar *MarketBar = nil 
	var lastClosePrice float64 = -1
	var lastTimestampUnit int64 = -1
	ticker := time.NewTicker(this.barDuration)
	barDurationMsec := int64(this.barDuration / time.Millisecond)
	this.logger.Info("Ready")
	
	for {

		select {
			
			case marketTimedEvent, isOpened := <- this.ingressCh:
			
				if !isOpened {
					this.logger.Info("Ingress channel closed, done")
					
					return
				}
				
				this.logger.Debugf("Received market timed event: %v", marketTimedEvent)
				
				if currentMarketBar == nil {
					currentMarketBar = new(MarketBar)
					currentMarketBar.ClosePrice = marketTimedEvent.MarketEvent.Price
					lastTimestampUnit = marketTimedEvent.TimestampMsec / barDurationMsec
					currentMarketBar.TimestampMsec = lastTimestampUnit * barDurationMsec
					
					if lastClosePrice == -1 {
						currentMarketBar.OpenPrice = marketTimedEvent.MarketEvent.Price
					
					} else {
						currentMarketBar.OpenPrice = lastClosePrice
					}
					
					currentMarketBar.HighPrice = math.Max(currentMarketBar.OpenPrice, marketTimedEvent.MarketEvent.Price)
					currentMarketBar.LowPrice = math.Min(currentMarketBar.OpenPrice, marketTimedEvent.MarketEvent.Price)					

				} else {
					currentMarketBar.HighPrice = math.Max(currentMarketBar.HighPrice, marketTimedEvent.MarketEvent.Price)
					currentMarketBar.LowPrice = math.Min(currentMarketBar.LowPrice, marketTimedEvent.MarketEvent.Price)
					currentMarketBar.ClosePrice = marketTimedEvent.MarketEvent.Price
				}		
				
				lastClosePrice = marketTimedEvent.MarketEvent.Price		
			
			case <- ticker.C:
			
				if lastClosePrice > -1 {

					if currentMarketBar == nil {
						currentMarketBar = new(MarketBar)
						currentMarketBar.HighPrice = lastClosePrice
						currentMarketBar.LowPrice = lastClosePrice
						currentMarketBar.ClosePrice = lastClosePrice
						currentMarketBar.OpenPrice = lastClosePrice
						lastTimestampUnit = lastTimestampUnit + 1
						currentMarketBar.TimestampMsec = lastTimestampUnit * barDurationMsec
					}
					
					currentMarketBar.BodySize = math.Abs(currentMarketBar.OpenPrice - currentMarketBar.ClosePrice)
					currentMarketBar.BarSize = currentMarketBar.HighPrice - currentMarketBar.LowPrice
					currentMarketBar.TailSize = currentMarketBar.BarSize - currentMarketBar.BodySize
					
					this.logger.Debugf("Finished aggregating market bar: %v", currentMarketBar)
					
					this.egressCh <- currentMarketBar
					currentMarketBar = nil
				}
		}
	}
}

//=====================================================================================================================
type MarketBar struct {
	OpenPrice float64
	ClosePrice float64
	LowPrice float64
	HighPrice float64
	BodySize float64
	TailSize float64
	BarSize float64
	TimestampMsec int64
}
