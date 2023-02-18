import os
from jinja2 import Environment, FileSystemLoader


class TemplateCompiler():
    """
    Compiles an HTML template with a context into an HTML file
    """
    def __init__(
            self,
            TemplateRootFolder: str = "./",
            TemplatePath: str = "",
            Context: dict = {}):
        self.template_root_folder = TemplateRootFolder
        self.template_path = TemplatePath
        self.context = Context

        self.jinja_env = Environment(
            loader=FileSystemLoader(self.template_root_folder)
        )

    def compile(self, output_path: str):
        full_output_path = self.template_root_folder + output_path

        os.makedirs(os.path.dirname(full_output_path), exist_ok=True)

        compilation_result = self.jinja_env \
            .get_template(self.template_path) \
            .stream(self.context) \
            .dump(full_output_path)

        print(f"Compiled to {full_output_path}")

        return compilation_result
