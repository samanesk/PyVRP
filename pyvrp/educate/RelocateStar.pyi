from pyvrp.PenaltyManager import PenaltyManager
from pyvrp.ProblemData import ProblemData

class RelocateStar(RouteOperator):
    def __init__(
        self, data: ProblemData, penalty_manager: PenaltyManager
    ) -> None: ...

class RouteOperator:
    def __init__(self, *args, **kwargs) -> None: ...