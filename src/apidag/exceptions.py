class DAGValidationError(Exception):
    pass

class DAGExecutionError(Exception):
    pass

class TemplateVariablesMissingError(Exception):
    pass

class ExcessTemplateVariablesError(Exception):
    pass

class ResultExtractionError(Exception):
    pass