// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
                    <input style={{width: "142px", position:"absolute", right: "10px"}}
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