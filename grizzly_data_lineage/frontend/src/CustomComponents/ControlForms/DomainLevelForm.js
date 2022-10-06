import ControlForm, { makeInputState, DynamicDropDown } from "./ControlForm";

class DomainLevelForm extends ControlForm {
    constructor(props) {
        super(props);
        this.state = {
            project: makeInputState({ name: "project", displayName: "Project", dependentInputs: ["datetime"] }),
            datetime: makeInputState({ name: "datetime", displayName: "Datetime" }),
            requestURL: "/parse_grizzly_domain",
            immediateUpdateInputs: ["project"],
            requestInputs: ["project", "datetime"],
            formLabel: "DOMAIN LEVEL",
        }
    }

    getInputUpdateURL = (inputName) => {
        switch (inputName) {
            case "project":
                return this.buildURLwithArgs("/get_projects", []);
            case "datetime":
                return this.buildURLwithArgs("/get_build_datetimes", ["project"]);
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

export default DomainLevelForm;