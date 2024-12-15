from dataclasses import dataclass
from typing import Optional

@dataclass
class Position:
    symbol: str
    side: str  # 'long' or 'short'
    size: float
    entry_price: float
    stop_loss_price: float
    take_profit_price: float
    timestamp: int
    leverage: int
    
    # 추가되는 필드들
    break_even_price: float
    unrealized_pl: float
    margin_size: float
    available: float
    locked: float
    liquidation_price: float
    margin_ratio: float
    mark_price: float
    achieved_profits: float  # 실현 손익
    total_fee: float        # 누적 펀딩비
    margin_mode: str        # 'isolated' or 'crossed'
    
    @property
    def is_long(self) -> bool:
        return self.side == 'long'
    
    @property
    def is_short(self) -> bool:
        return self.side == 'short'
        
    @property
    def total_position_value(self) -> float:
        """포지션의 총 가치 계산"""
        return self.size * self.mark_price

    @property
    def roi_percentage(self) -> float:
        """수익률 계산"""
        if self.is_long:
            return ((self.mark_price - self.entry_price) / self.entry_price) * 100
        else:
            return ((self.entry_price - self.mark_price) / self.entry_price) * 100