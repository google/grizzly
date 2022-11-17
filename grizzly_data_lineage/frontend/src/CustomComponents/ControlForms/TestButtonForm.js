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

class TestButtonForm extends ControlForm {
    constructor(props) {
        super(props);
        this.state = {
            test_name: makeInputState({ name: "test_name", displayName: "Test" }),
            physical: makeInputState({ name: "physical", displayName: "Physical", value: "false", possibleValues: ["false", "true"] }),
            requestURL: "/parse_test",
            requestInputs: ["test_name", "physical"],
            immediateUpdateInputs: ["test_name"],
            formLabel: "TEST",
        };
    }

    getInputUpdateURL = (inputName) => {
        switch (inputName) {
            case "test_name":
                return this.buildURLwithArgs("/get_tests", []);
            default:
                return null;
        }
    }

    getFormContents = () => {
        return (
            <>
                <DynamicDropDown
                    state={this.state.test_name}
                    onChange={this.handleInputChange}
                /><br />
                <DynamicDropDown
                    state={this.state.physical}
                    onChange={this.handleInputChange}
                /><br />
                <input type="submit" value="Submit" />
            </>
        );
    }
}

export default TestButtonForm;