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
    
    @property
    def is_long(self) -> bool:
        return self.side == 'long'
    
    @property
    def is_short(self) -> bool:
        return self.side == 'short'