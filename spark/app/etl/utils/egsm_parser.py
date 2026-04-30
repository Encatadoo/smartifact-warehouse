import xml.etree.ElementTree as ET

NS = {"ca": "http://siena.ibm.com/model/CompositeApplication"}


def parse_egsm_hierarchy(egsm_xml_string):
    """
    Parse EGSM XML and extract stage hierarchy with leaf detection.
    
    A stage is a leaf if it contains no ca:SubStage children.
    
    Returns:
        dict: {
            stage_id: {
                "parent": parent_stage_id or None,
                "is_leaf": bool
            }
        }
        
    Example for LHR-AMS Truck perspective:
        {
            "example":        {"parent": None,      "is_leaf": False},
            "LoadContainer":  {"parent": "example", "is_leaf": True},
            "TravelUK":      {"parent": "SequenceFlow_3", "is_leaf": True},
            ...
        }
    """
    root = ET.fromstring(egsm_xml_string)
    hierarchy = {}

    gsm = root.find(".//ca:GuardedStageModel", NS)
    if gsm is None:
        return hierarchy

    for stage in gsm.findall("ca:Stage", NS):
        stage_id = stage.get("id")
        has_children = len(stage.findall("ca:SubStage", NS)) > 0
        hierarchy[stage_id] = {"parent": None, "is_leaf": not has_children}
        _parse_substages_recursive(stage, stage_id, hierarchy)

    return hierarchy


def _parse_substages_recursive(parent_element, parent_id, hierarchy):
    """Recursively walk SubStage elements, recording parent and leaf status."""
    for substage in parent_element.findall("ca:SubStage", NS):
        substage_id = substage.get("id")
        has_children = len(substage.findall("ca:SubStage", NS)) > 0
        hierarchy[substage_id] = {"parent": parent_id, "is_leaf": not has_children}
        _parse_substages_recursive(substage, substage_id, hierarchy)