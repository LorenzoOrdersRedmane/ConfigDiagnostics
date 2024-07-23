from __future__ import annotations
import re
import json

REGEX_FOR_FINDING_MIRRORED_FIELDS = r"(?<=\{\[)[\w|:]+(?=\]\})"

class Datalist:
    def __init__(self, sysname: str) -> None:
        self.sysname: str = sysname
        self.parents: list[Datalist] = []
        self.children: list[Datalist] = []
        self.fields: dict[str, Field] = dict()
        self.workflows: dict[str, Workflow] = dict()
        self.raw_json: dict = dict()
        self.error_messages: list[str] = []

    def __str__(self) -> str:
        return self.sysname
    
    def fetch_error_messages(self) -> list[str]:
        return self.error_messages + sum(map(lambda f: self.fields[f].fetch_error_messages(), self.fields), []) + sum(map(lambda f: self.workflows[f].fetch_error_messages(), self.workflows), [])

    def create_datalists_from_jsons(datalist_jsons: list[dict]) -> dict[str, Datalist]:
        datalists: dict[str, Datalist] = {}
        for datalist_json in datalist_jsons:
            datalist_name = datalist_json["SystemName"]
            new_datalist = Datalist(datalist_name)
            new_datalist.raw_json = datalist_json

            for field_json in datalist_json["Fields"]:
                field_name = field_json["SystemName"]
                new_field = Field(new_datalist, field_name)
                new_field.raw_json = field_json
                new_datalist.fields[field_name] = new_field

            for workflow in datalist_json["Workflows"]:
                workflow_label = workflow["EventName"]
                new_workflow = Workflow(new_datalist, workflow_label)
                new_workflow.raw_json = workflow
                new_datalist.workflows[workflow_label] = new_workflow

            datalists[datalist_name] = new_datalist
        for datalist_json in datalist_jsons:
            this_datalist_name = datalist_json["SystemName"]
            this_datalist = datalists[this_datalist_name]
            fields = datalist_json["Fields"]

            for field in fields:
                field_name = field["SystemName"]
                this_field = this_datalist.fields[field_name]

                # populate field options
                if "FieldOptions" in field and field["FieldOptions"] is not None:
                    this_field.field_options = json.loads(field["FieldOptions"])

                # Populate dynamic fields
                dynamic_data = field["DynamicData"]
                if dynamic_data is not None:
                    dynamic_source = dynamic_data["DynamicSourceSystemName"]
                    cascading_source = dynamic_data["CascadingSystemName"]
                    if dynamic_source is not None:
                        this_field.dynamic_source = dynamic_source
                    if cascading_source is not None:
                        this_field.cascading_source = cascading_source
                
                # field dependency
                if field["DependsOn"] is not None:
                    dependent_on_field = field["DependsOn"]["SystemName"]
                    this_field.depends_on = this_datalist.fields[dependent_on_field]


            # connect datalists based on parent-child relationships
            relationships = datalist_json["Relationships"]
            for relationship in relationships:
                parent_datalist_name = relationship["ParentSystemName"]
                child_datalist_name = relationship["ChildSystemName"]
                if parent_datalist_name is not None and parent_datalist_name in datalists:
                    parent_datalist = datalists[parent_datalist_name]
                    child_datalist = datalists[this_datalist_name]
                if parent_datalist_name is not None and parent_datalist_name not in datalists:
                    datalists[this_datalist_name].error_messages.append(f"Invalid reference to parent datalist {parent_datalist_name}")

                if child_datalist_name is not None and child_datalist_name in datalists:
                    parent_datalist = datalists[this_datalist_name]
                    child_datalist = datalists[child_datalist_name]
                if child_datalist_name is not None and child_datalist_name not in datalists:
                    datalists[this_datalist_name].error_messages.append(f"Invalid reference to child datalist {child_datalist_name}")

                parent_datalist.children.append(child_datalist)
                child_datalist.parents.append(parent_datalist)
            
            # connect workflows and dependent fields
            workflows = datalist_json["Workflows"]
            for workflow_data in workflows:
                workflow_name = workflow_data["EventName"]
                workflow = this_datalist.workflows[workflow_name]
                mandatory_parent = workflow_data["ParentListSystemName"]
                if mandatory_parent is not None and mandatory_parent in datalists:
                    workflow.mandatory_parent = datalists[mandatory_parent]
                if mandatory_parent is not None and mandatory_parent not in datalists:
                    workflow.error_messages.append(f"Invalid reference to mandatory parent {mandatory_parent}")

                for workflow_data in workflow_data["Fields"]:
                    field_name = workflow_data["FieldName"]
                    field_filter_type = workflow_data["Type"]
                    # a field can be either from this datalist or from its mandatory parent
                    if field_name in this_datalist.fields:
                        workflow_field = this_datalist.fields[field_name]
                    elif workflow.mandatory_parent is not None:
                        workflow_field = workflow.mandatory_parent.fields[field_name]

                    if field_filter_type == "Filter":
                        workflow.filter_fields.append(workflow_field)
                    elif field_filter_type == "Submission":
                        workflow.submission_fields.append(workflow_field)
                    elif field_filter_type == "Success":
                        workflow.success_fields.append(workflow_field)
                    elif field_filter_type == "Failure":
                        workflow.failure_fields.append(workflow_field)
            
        # mirrored fields
        for datalist_json in datalist_jsons:
            this_datalist_name = datalist_json["SystemName"]
            this_datalist = datalists[this_datalist_name]
            fields = datalist_json["Fields"]
            for field_data in fields:
                field_name = field_data["SystemName"]
                this_field = this_datalist.fields[field_name]
                this_field_default_value = field_data["DefaultValue"]
                if this_field_default_value is not None:
                    default_values = re.findall(REGEX_FOR_FINDING_MIRRORED_FIELDS, field_data["DefaultValue"])
                    for default_value in default_values:
                        # this contains all the datalists that the mirrored field could come from
                        datalists_of_field_value = [this_datalist]
                        # this contains all the fields that are the source of the mirrored field
                        mirrored_field_sources: list[Field] = []
                        for mirrored_field_name in default_value.split(":"):
                            # a 'parent' mirror means to go to the parent datalist for the next field
                            if mirrored_field_name == "parent":
                                # if no parent exists, then this field has an invalid mirror
                                if any(map(lambda f: len(f.parents) == 0, datalists_of_field_value)):
                                    mirrored_field_sources = []
                                    this_field.error_messages.append(f"Invalid mirror to {mirrored_field_name}")
                                    break
                                datalists_of_field_value = list(sum(map(lambda g: g.parents, datalists_of_field_value), []))
                            else:
                                # find the field object for the current mirrored system name
                                # if the mirrored field is not in the datalists, then it's not a valid mirror
                                if any(map(lambda f: mirrored_field_name not in f.fields, datalists_of_field_value)):
                                    mirrored_field_sources = []
                                    this_field.error_messages.append(f"Invalid mirror to {mirrored_field_name}")
                                    break
                                mirrored_field_sources = list(map(lambda f: f.fields[mirrored_field_name], datalists_of_field_value))
                                # if the mirrored field is a CDDD, DDD, or embedded list, we will keep going into that datalist
                                if all(map(lambda f: f.dynamic_source is not None, mirrored_field_sources)):
                                    datalists_of_field_value = list(map(lambda f: datalists[f.dynamic_source], mirrored_field_sources))
                        this_field.mirror_sources.extend(mirrored_field_sources)
                    this_field.mirror_sources = list(set(this_field.mirror_sources))
                    
        return datalists

class Field:
    def __init__(self, datalist: Datalist, sysname: str) -> None:
        self.datalist: Datalist = datalist
        self.sysname: str = sysname
        self.depends_on: Field = None
        self.mirror_sources: list[Field] = []
        self.cascading_source: Field = None
        self.dynamic_source: Datalist = None
        self.field_options: dict = None
        self.raw_json: dict = dict()
        self.error_messages: list[str] = []

    def __str__(self) -> str:
        return f"Datalist: {str(self.datalist)} Field: {self.sysname}"
    
    def __hash__(self) -> int:
        return hash(self.datalist.sysname + self.sysname)
    
    def fetch_error_messages(self) -> list[str]:
        return list(map(lambda f: f"Field {self.sysname}: {f}", self.error_messages))

class Workflow:
    def __init__(self, datalist: Datalist, label: str) -> None:
        self.datalist: Datalist = datalist
        self.label: str = label
        self.filter_fields: list[Field] = []
        self.success_fields: list[Field] = []
        self.failure_fields: list[Field] = []
        self.submission_fields: list[Field] = []
        self.mandatory_parent: Datalist = None
        self.raw_json: dict = dict()
        self.error_messages: list[str] = []

    def __str__(self) -> str:
        return f"Datalist: {str(self.datalist)} Field: {self.label}"
    
    def fetch_error_messages(self) -> list[str]:
        return list(map(lambda f: f"Field {self.label}: {f}", self.error_messages))
