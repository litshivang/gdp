# adapter plugins — all datasets registry-driven

from app.ingestion.core.registry import registry
from app.ingestion.adapters.national_gas import NationalGasAdapter
from app.ingestion.adapters.entsog import EntsogAdapter
from app.ingestion.adapters.gie_agsi import GieAgsiAdapter
from app.ingestion.adapters.gie_alsi import GieAlsiAdapter
from app.ingestion.adapters.instantaneous_flow import InstantaneousFlowAdapter
from app.ingestion.adapters.gas_publications import GasPublicationsAdapter
from .bmrs_fuelhh import BmrsFuelHHAdapter
from .bmrs_demand_outturn import BmrsDemandOutturnAdapter

registry.register("GAS_QUALITY", NationalGasAdapter)
registry.register("ENTSOG", EntsogAdapter)
registry.register("AGSI", GieAgsiAdapter)
registry.register("ALSI", GieAlsiAdapter)
registry.register("INSTANTANEOUS_FLOW", InstantaneousFlowAdapter)
registry.register("GAS_PUBLICATIONS", GasPublicationsAdapter)
registry.register("FUELHH", BmrsFuelHHAdapter)
registry.register("DEMAND_OUTTURN", BmrsDemandOutturnAdapter)
