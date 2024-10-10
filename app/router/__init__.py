from ..resources.demandmaster import DemandMaster
from ..resources.costmaster import CostMaster
from ..resources.destinationmaster import DestinationMaster
from ..resources.supplymaster import SupplyMaster
from ..resources.warehousemaster import WarehouseMaster
from ..resources.plantmaster import PlantMaster
from ..resources.salespricemaster import SalespriceMaster
from ..resources.subproductmaster import SubproductMaster
from ..resources.pdfreightmaster import PdfreightMaster
from ..resources.pwfreightmaster import PwfreightMaster
from ..resources.wdfreightmaster import WdfreightMaster
from ..resources.merge_data import MergedData
from ..resources.fetch_history_details import HistoryDetails
from ..resources.read_data import ReadData
from ..resources.download_data import DownloadData

def register_routes(api):
    api.add_resource(DemandMaster,"/upload/demandmaster")
    api.add_resource(CostMaster,"/upload/costmaster")
    api.add_resource(DestinationMaster,"/upload/destinationmaster")
    api.add_resource(SupplyMaster,"/upload/supplymaster")
    api.add_resource(WarehouseMaster,"/upload/warehousemaster")
    api.add_resource(PlantMaster,"/upload/plantmaster")
    api.add_resource(SalespriceMaster,"/upload/salespricemaster")
    api.add_resource(SubproductMaster,"/upload/subproductmaster")
    api.add_resource(PdfreightMaster,"/upload/pdfreightmaster")
    api.add_resource(PwfreightMaster,"/upload/pwfreightmaster")
    api.add_resource(WdfreightMaster,"/upload/wdfreightmaster")
    api.add_resource(MergedData,"/run_logic")
    api.add_resource(HistoryDetails,"/history_details")
    api.add_resource(ReadData,"/read_data")
    api.add_resource(DownloadData,"/download_data")