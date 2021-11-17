from controller.pipelines import factory, run
from controller.tasks import create_tasks


def main(request):
    data = request.get_json()
    print(data)

    if "task" in data:
        response = create_tasks(data)
    elif "table" in data and "ads_account_id" in data:
        response = run(
            factory(data["table"]),
            data["ads_account_id"],
            data.get("start"),
            data.get("end"),
        )
    else:
        raise ValueError(data)

    print(response)
    return response
