class Consumer:
    """
    imports, runner and main are not required in this version
    The payload cannot be generated unless bool(self) is True
    """

    file_path: str = ""
    # imports: str = ""
    field_list: str = ""
    row_parser: str = ""
    # runner: str = ""
    # main: str = ""
    payload: str = ""

    def __bool__(self):
        return self.file_path != "" and self.field_list != "" and self.row_parser != ""
