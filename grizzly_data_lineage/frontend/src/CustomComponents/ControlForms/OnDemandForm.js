import ControlForm, { makeInputState, DynamicDropDown } from "./ControlForm";

class OnDemandForm extends ControlForm {
    constructor(props) {
        super(props);
        this.state = {
            project: makeInputState({ name: "project", displayName: "Project" }),
            domain: makeInputState({ name: "domain", displayName: "Domain" }),
            physical: makeInputState({ name: "physical", displayName: "Physical", value: "false", possibleValues: ["false", "true"] }),
            requestURL: "/parse_grizzly_on_demand",
            immediateUpdateInputs: ["project"],
            requestInputs: ["project", "domain", "physical"],
            formLabel: "ON DEMAND",
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
                <label>
                    {this.state.domain.displayName}:
                    <input
                        name={this.state.domain.name}
                        type="text"
                        value={this.state.domain.value}
                        onChange={this.handleInputChange} />
                </label><br />
                <DynamicDropDown
                    state={this.state.physical}
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

export default OnDemandForm;