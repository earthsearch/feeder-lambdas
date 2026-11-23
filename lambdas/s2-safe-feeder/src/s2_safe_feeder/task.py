from typing import Any

from stactask import Task


class S2SafeFeeder(Task):
    name = "s2-safe-feeder"
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
    return S2SafeFeeder.handler(payload=event)


if __name__ == "__main__":
    S2SafeFeeder.cli()
