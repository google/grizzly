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

import { Component } from 'react';

import { DynamicDropDown, makeInputState } from './ControlForm';

class TableSearchForm extends Component {
    constructor(props) {
        super(props);
        this.state = {
            table: makeInputState({ name: "table", displayName: "Table" }),
            formLabel: "TABLE SEARCH",
        };
    }

    updateTableList(list) {
        this.setState({table: {...this.state.table, possibleValues: [...list]}});
    }

    resetTable() {
        this.setState({table: {...this.state.table, value: ""}});
    }

    handleInputChange = (event) => {
        const target = event.target;
        const name = target.name;
        const value = target.type === 'checkbox' ? target.checked : target.value;
        this.setState({ [name]: { ...this.state[name], value: value } });
    }

    handleSubmit = (event) => {
        event?.preventDefault();
        this.props.focusOnTable(this.state.table.value);
    }

    clearInputs = () => {
        this.setState({table: {...this.state.table, value: "", possibleValues: []}})
    }

    render = () => {
        return (
            <form onSubmit={this.handleSubmit} >
                <fieldset disabled={this.props.formLock}>
                    <label><b>{this.state.formLabel}:</b></label><br />
                    <DynamicDropDown
                        state={this.state.table}
                        onChange={this.handleInputChange}
                    /><br />
                    <input type="submit" value="Submit" />
                </fieldset>
            </form>
        );
    }
}

export default TableSearchForm;