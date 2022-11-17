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

class DomainLevelForm extends ControlForm {
    constructor(props) {
        super(props);
        this.state = {
            project: makeInputState({ name: "project", displayName: "Project", dependentInputs: ["datetime"] }),
            datetime: makeInputState({ name: "datetime", displayName: "Datetime", dependentInputs: ["domain"] }),
            domain: makeInputState({ name: "domain", displayName: "Domain" }),
            requestURL: "/parse_grizzly_domain",
            immediateUpdateInputs: ["project"],
            requestInputs: ["project", "datetime", "domain"],
            formLabel: "DOMAIN LEVEL",
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