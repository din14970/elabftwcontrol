from enum import Enum
from pathlib import Path

import git
from pydantic import BaseModel

from elabftwcontrol._logging import logger
from elabftwcontrol.client import ElabftwApi
from elabftwcontrol.upload.jobs import WorkEvaluator
from elabftwcontrol.upload.parsers import ManifestParser
from elabftwcontrol.upload.state import State


class ApplyJobType(str, Enum):
    CREATE = "create"
    DESTROY = "destroy"


class ApplyJobConfig(BaseModel):
    jobtype: ApplyJobType
    manifest_folder: Path | str
    ignore_state: bool
    state_file: Path | str | None
    version: str | None
    n_retries: int
    no_prompt: bool
    dry_run: bool


class ApplyJob:
    def __init__(
        self,
        api: ElabftwApi,
        config: ApplyJobConfig,
    ) -> None:
        self.api = api
        self.config = config

    def __call__(self) -> None:
        parser = ManifestParser(self.config.manifest_folder)
        manifest_index = parser.parse()

        state: State
        if self.config.ignore_state:
            logger.debug("Ignoring state.")
            state = State.empty()
        else:
            if self.config.state_file is None:
                logger.debug("Pulling state from the API...")
                state = State.from_api(self.api, skip_untracked=True)
            else:
                logger.debug("Getting state from a file.")
                state = State.from_file(self.config.state_file)

        version: str = ""
        if self.config.jobtype == ApplyJobType.CREATE:
            if self.config.version is None:
                try:
                    repo = git.Repo(
                        self.config.manifest_folder,
                        search_parent_directories=True,
                    )
                    version = repo.head.object.hexsha
                    assert version is not None
                    logger.info(
                        "Version %s was detected from git and will be used in the apply."
                        % version
                    )
                except Exception:
                    logger.warning(
                        "No version was passed or could be detected from version control."
                    )
                    version = ""
            else:
                version = self.config.version
                logger.info("Using version %s in the apply." % version)

        evaluator = WorkEvaluator.new(
            manifest_index=manifest_index,
            elab_state=state,
            version=version,
        )

        if self.config.jobtype == ApplyJobType.CREATE:
            plan = evaluator.evaluate_apply()
        elif self.config.jobtype == ApplyJobType.DESTROY:
            plan = evaluator.evaluate_destroy()
        else:
            raise RuntimeError(f"Could not identify job type: {self.config.jobtype}")

        if not plan:
            print("No work to be done!")
            return

        print("The following is the work to be done:")
        print()

        plan.print_to_console()

        print()
        print()

        if not self.config.no_prompt:
            if input("Do you wish to apply the changes? [Y|n] ").startswith("n"):
                return

        if not self.config.dry_run:
            plan.execute(
                client=self.api,
                start=0,
                n_retries=self.config.n_retries,
            )

        print("Completed apply.")
