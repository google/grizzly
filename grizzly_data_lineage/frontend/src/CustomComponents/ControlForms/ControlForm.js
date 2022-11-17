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
import axios from "axios";

export function makeInputState({ name, displayName = "", value = "", possibleValues = [], dependentInputs = [] }) {
    return {
        name: name,
        displayName: displayName,
        value: value,
        possibleValues: possibleValues,
        dependentInputs: dependentInputs,
        loaded: false,
    }
}


export class DynamicDropDown extends Component {
    formatInputOptions = () => {
        let values = [...this.props.state.possibleValues];
        values.sort();
        if (this.props.state.name === "datetime") {
            values.reverse();
        }
        values = ["", ...values]
        return values.map((v) => {
            return <option value={v} key={v}>{v}</option>
        });
    }

    render = () => {
        return (
            <label>
                {this.props.state.displayName}:
                <select style={{width: "150px", position:"absolute", right: "10px"}}
                    name={this.props.state.name}
                    value={this.props.state.value}
                    onChange={this.props.onChange}>
                    {this.formatInputOptions()}
                </select>
            </label>
        );
    }
}

class ControlForm extends Component {
    constructor(props) {
        super(props);
        this.state = {
            autosubmitPending: true
        };
    }

    parseURLQuery = () => {
        let newState = {};
        for (let k of this.props.searchParams?.keys()) {
            let v = this.props.searchParams.get(k);
            newState[k] = {
                ...this.state[k],
                value: v,
            };
        }
        return newState;
    }

    checkAutoSubmit = (newState) => {
        for (let inputName of this.state.requestInputs) {
            if (newState[inputName] === "") {
                return false;
            }
        }
        return true;
    }

    checkSubmitReadiness = () => {
        for (let inputName of this.state.requestInputs) {
            if (!this.state[inputName].loaded) {
                return false;
            }
        }
        return true;
    }

    componentDidMount() {    
        if (this.props.searchParams.get("type") === this.state.formLabel) {
            const newState = this.parseURLQuery();
            newState.autosubmitPending = this.checkAutoSubmit(newState);
            this.setState(newState, () => {
                this.updateAllInputs();
            });
        } else {
            this.updateAllInputs()
        }
    } 

    getInputUpdateURL = (inputName) => { }
    getFormContents = () => { }

    buildURLwithArgs = (baseURL, argsList) => {
        let url = baseURL;
        if (argsList) {
            url += "?";
            argsList.forEach((argName) => {
                const arg = this.state[argName];
                url += `${arg.name}=${arg.value}&`
            })
        }
        return url;
    }

    getSubmitURL = () => {
        return this.buildURLwithArgs(this.state.requestURL, this.state.requestInputs);
    }

    makeRequest = (requestURL, callback, formData = {}) => {
        this.props.setFormLock(true);
        axios.get(requestURL, formData).then((response) => {
            // console.log("SUCCESS", response);
            callback(response.data);
            this.props.setFormLock(false);
        }).catch((error) => {
            console.log(error);
            this.props.setMessage("errorMessage", error.response.data);
            this.props.setFormLock(false);
        })
    }

    clearInput = (inputName) => {
        this.setState({ [inputName]: { ...this.state[inputName], value: "", possibleValues: [] } }, () => {
            this.state[inputName].dependentInputs.forEach((n) => { this.clearInput(n) });
        });
    }

    clearInputs = () => { }

    updateInput = (inputName) => {
        const updateURL = this.getInputUpdateURL(inputName);
        if (updateURL !== null) {
            this.makeRequest(updateURL, (data) => {
                let inputValue = this.state[inputName].value;
                // special case with datetime to support the selection of latest datetime
                if (inputName === "datetime" && inputValue === "latest") {
                    inputValue = data[0];
                }
                this.setState({ [inputName]: { ...this.state[inputName], possibleValues: [...data], value: inputValue, loaded: true } }, () => {
                    if (this.state[inputName].value) {
                        this.state[inputName].dependentInputs.forEach((n) => { this.updateInput(n) })
                    }
                    if (this.state.autosubmitPending) {
                        if (this.checkSubmitReadiness()) {
                            this.setState({ autosubmitPending: false }, () => { this.handleSubmit(null) });
                        }
                    }
                });
            })
        }
    }

    updateAllInputs = () => {
        this.state.immediateUpdateInputs.forEach((i) => {
            this.updateInput(i);
        });
    }

    handleInputChange = (event) => {
        this.props.setMessage("errorMessage", "");
        const target = event.target;
        const name = target.name;
        const value = target.type === 'checkbox' ? target.checked : target.value;
        this.setState({ [name]: { ...this.state[name], value: value } }, () => {
            this.state[name].dependentInputs.forEach((n) => { this.clearInput(n) });
            if (this.state[name].value) {
                this.state[name].dependentInputs.forEach((n) => { this.updateInput(n) });
            }
        });
    }

    getCurrentlyLoadedMessage = () => {
        return (
            <ul style={{marginTop: "-0.5px", marginBottom: "-0.5px"}}>
                <li key="Type">Type: {this.state.formLabel}</li>
                {this.state.requestInputs.map((i) => {
                    return (<li key={this.state[i].displayName}>{this.state[i].displayName}: {this.state[i].value}</li>)
                })}
            </ul>
        )
    }

    getFormParams = () => {
        let formParams = { type: this.state.formLabel };
        for (let input of this.state.requestInputs) {
            formParams[input] = this.state[input].value;
        }
        return formParams;
    }

    handleSubmit = (event) => {
        event?.preventDefault();
        this.props.setMessage("errorMessage", "");
        this.props.clearFlow(this.state.formLabel);
        this.makeRequest(this.getSubmitURL(), (data) => {
            this.props.loadFlow(data.objects, data.connections, this.getFormParams());
            this.props.setMessage("currentlyLoadedMessage", this.getCurrentlyLoadedMessage());
        })
    }

    render = () => {
        return (
            <form onSubmit={this.handleSubmit}>
                <fieldset disabled={this.props.formLock}>
                    <label><b>{this.state.formLabel}:</b></label><br/>
                    {this.getFormContents()}
                </fieldset>
            </form>
        );
    }
}

export default ControlForm;