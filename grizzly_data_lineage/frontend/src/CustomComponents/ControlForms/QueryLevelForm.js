import ControlForm, { makeInputState, DynamicDropDown } from "./ControlForm";

class QueryLevelForm extends ControlForm {
    constructor(props) {
        super(props);
        this.state = {
            project: makeInputState({ name: "project", displayName: "Project", dependentInputs: ["datetime"] }),
            datetime: makeInputState({ name: "datetime", displayName: "Datetime", dependentInputs: ["domain"] }),
            domain: makeInputState({ name: "domain", displayName: "Domain", dependentInputs: ["job_build_id"] }),
            job_build_id: makeInputState({ name: "job_build_id", displayName: "Job Build ID" }),
            requestURL: "/parse_grizzly_query",
            immediateUpdateInputs: ["project"],
            requestInputs: ["project", "datetime", "domain", "job_build_id"],
            formLabel: "QUERY LEVEL",
        }
    }

    getInputUpdateURL = (inputName) => {
        switch (inputName) {
            case "project":
                return this.buildURLwithArgs("/get_projects", []);
            case "datetime":
                return this.buildURLwithArgs("/get_build_datetimes", ["project"]);
            case "domain":
                return this.buildURLwithArgs("/get_domains", ["project", "datetime"]);
            case "job_build_id":
                return this.buildURLwithArgs("/get_job_build_ids", ["project", "datetime", "domain"]);
            default:
                return null;
        }
    }

    getFormContents = () => {
        return (
            <>
                <DynamicDropDown
                    state={this.state.project}
                    onChange={this.handleInputChange}
                /><br />
                <DynamicDropDown
                    state={this.state.datetime}
                    onChange={this.handleInputChange}
                /><br />
                <DynamicDropDown
                    state={this.state.domain}
                    onChange={this.handleInputChange}
                /><br />
                <DynamicDropDown
                    state={this.state.job_build_id}
                    onChange={this.handleInputChange}
                /><br />
                <input type="submit" value="Submit" />
            </>
        );
    }

    clearInputs = () => {
        this.handleInputChange({
            target: {
                name: "project",
                value: "",
                type: "DynamicDropDown"
            }
        });
    }
}

export default QueryLevelForm;