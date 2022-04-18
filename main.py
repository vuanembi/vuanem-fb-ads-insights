from typing import Any

from facebook.facebook_controller import facebook_controller
from tasks.tasks_service import tasks_service


def main(request):
    data: dict[str, Any] = request.get_json()
    print(data)

    if "table" in data and "ads_account_id" in data:
        fn = facebook_controller
    elif "task" in data:
        fn = tasks_service
    else:
        raise ValueError(data)

    response = fn(data)
    print(response)
    return response
