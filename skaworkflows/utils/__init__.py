from skaworkflows.utils.analysis import (
    heatmap_computing_permutations,
    plan_weighting_demonstration,
)


def create_observations_from_config(cfg) -> list:
    from skaworkflows.observation.observation import Observation

    telescope = cfg.get("instrument", {}).get("telescope", {})
    pipelines = telescope.get("pipelines", {})
    observations = telescope.get("observations", [])

    obs_by_name = {
        obs["name"]: obs
        for obs in observations
        if isinstance(obs, dict) and "name" in obs
    }

    unified = []

    for name, pipeline_data in pipelines.items():
        obs_data = obs_by_name.get(name, {})

        start = obs_data.get("start", 0)
        duration = obs_data.get("duration", pipeline_data.get("duration"))
        end = start + duration if start is not None and duration is not None else None

        merged = {
            "name": name,
            "start": start,
            "duration": duration,
        }

        if isinstance(pipeline_data, dict):
            merged.update(pipeline_data)

        if isinstance(obs_data, dict):
            merged.update(obs_data)

        merged["name"] = name
        merged["start"] = start
        merged["duration"] = duration

        unified.append(merged)

    updated_observations = []
    for record in unified:
        hpso = record.get("type") or record["name"].split("_")[0]

        obs = Observation(
            name=record["name"],
            hpso=hpso,
            workflows=record.get("workflow_type", []),
            demand=record.get("instrument_demand", record.get("demand")),
            duration=record.get("duration"),
            channels=record.get("channels"),
            workflow_parallelism=record.get("workflow_parallelism"),
            baseline=record.get("baseline"),
            telescope=telescope,
            start=record.get("start", 0),
        )

        obs.workflow_path = record.get("workflow")
        obs.ingest_compute_demand = record.get("ingest_demand")
        obs.ingest_data_rate = record.get("data_product_rate")

        updated_observations.append(obs)

    return updated_observations
