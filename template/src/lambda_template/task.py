from typing import Any

from stactask import Task


class LambdaTemplate(Task):
    name = "lambda-template"
    description = "TODO: describe this lambda"

    def validate(self) -> bool:
        return True

    def process(self, **kwargs: Any) -> list[dict[str, Any]]:
        # TODO: replace with this lambda's logic. The check below is a stand-in that
        # demonstrates the `tests/fixtures/payloads/failure` pattern - remove it along
        # with its fixture once real processing logic is added.
        if "invalid" in self.process_definition.get("id", ""):
            raise Exception("invalid")

        return self.items_as_dicts


def lambda_handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    return LambdaTemplate.handler(payload=event)


if __name__ == "__main__":
    LambdaTemplate.cli()
