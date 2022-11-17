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

import React, { useState, useRef} from 'react';

import ProjectLevelForm from './ControlForms/ProjectLevelForm';
import DomainLevelForm from './ControlForms/DomainLevelForm';
import QueryLevelForm from './ControlForms/QueryLevelForm';
import OnDemandForm from './ControlForms/OnDemandForm';
import TableSearchForm from './ControlForms/TableSearchForm';
import TestButtonForm from './ControlForms/TestButtonForm';


import {getColorHelpText} from "./../Utils/HelpMenu";

const FlowControls = ({ debug, searchParams, clearFlow, loadFlow, focusOnTable, TSRef }) => {
    const [controlFormLock, setControlFormLock] = useState();
    const updateControlFormLock = (lock) => { setControlFormLock(lock); }
    const [controlsHidden, setControlsHidden] = useState(false);

    const [errorMessage, setErrorMessage] = useState();
    const [currentlyLoadedMessage, setCurrentlyLoadedMessage] = useState();
    const setMessage = (name, msg) => {
        switch (name) {
            case "errorMessage":
                setErrorMessage(msg);
                break;
            case "currentlyLoadedMessage":
                setCurrentlyLoadedMessage(msg);
                break;
            default:
                return;
        }
    }
    const getMessage = (name) => {
        switch (name) {
            case "errorMessage":
                return errorMessage ? `Error: ${errorMessage}` : ""
            case "currentlyLoadedMessage":
                return currentlyLoadedMessage ? currentlyLoadedMessage : <><br/>No Graph is currently loaded<br/></>;
            default:
                return;
        }
    }

    const projectFormRef = useRef();
    const domainFormRef = useRef();
    const queryFormRef = useRef();
    const onDemandFormRef = useRef();
    const testFormRef = useRef();
    let formRefs = [projectFormRef, domainFormRef, queryFormRef, TSRef];
    if (debug) {
        formRefs = [...formRefs, onDemandFormRef, testFormRef]
    }

    const clearFlowAndForms = (formType) => {
        clearFlow();
        setMessage("currentlyLoadedMessage", "");
        setMessage("errorMessage", "");
        formRefs.forEach((form) => {if (form.current.state.formLabel !== formType) {
            form.current.clearInputs();
        }})
    }

    const projectForm = (
        <ProjectLevelForm
            formLock={controlFormLock}
            setFormLock={updateControlFormLock}
            setMessage={setMessage}
            searchParams={searchParams}
            clearFlow={clearFlowAndForms}
            loadFlow={loadFlow}
            ref={projectFormRef}
        />
    );
    const domainForm = (
        <DomainLevelForm
            formLock={controlFormLock}
            setFormLock={updateControlFormLock}
            setMessage={setMessage}
            searchParams={searchParams}
            clearFlow={clearFlowAndForms}
            loadFlow={loadFlow}
            ref={domainFormRef}
        />
    );
    const queryForm = (
        <QueryLevelForm
            formLock={controlFormLock}
            setFormLock={updateControlFormLock}
            setMessage={setMessage}
            searchParams={searchParams}
            clearFlow={clearFlowAndForms}
            loadFlow={loadFlow}
            ref={queryFormRef}
        />
    );
    const onDemandForm = (
        <OnDemandForm
            formLock={controlFormLock}
            setFormLock={updateControlFormLock}
            setMessage={setMessage}
            searchParams={searchParams}
            clearFlow={clearFlowAndForms}
            loadFlow={loadFlow}
            ref={onDemandFormRef}
        />
    );
    const testForm = (
        <TestButtonForm
            formLock={controlFormLock}
            setFormLock={updateControlFormLock}
            setMessage={setMessage}
            searchParams={searchParams}
            clearFlow={clearFlowAndForms}
            loadFlow={loadFlow}
            ref={testFormRef}
        />
    );
    const tableSearchForm = (
        <TableSearchForm
            formLock={controlFormLock}
            focusOnTable={focusOnTable}
            ref={TSRef}
        />
    );
    
    return (
        <div className="controls-node" style={{ position: 'fixed', left: 10, top: 10, zIndex: 4, backgroundColor: "#ffffff", textAlign: "left" }}>
            <button onClick={() => { setControlsHidden(!controlsHidden) }}>Show/Hide Forms</button>
            <div hidden={controlsHidden} style={{width: "300px"}}>
                <fieldset disabled={controlFormLock} style={{width: "268px"}}>
                    <div style={{width: "268px", overflowWrap: "break-word"}}>
                        <b>CURRENT GRAPH INFO:</b>
                        {getMessage("currentlyLoadedMessage")}
                        <button onClick={clearFlowAndForms}>Clear Graph</button>
                    </div>
                </fieldset>
                {projectForm}
                {domainForm}
                {queryForm}
                {debug && onDemandForm}
                {debug && testForm}
                {tableSearchForm}
                {getColorHelpText()}
                <div style={{ maxWidth: "300px", color: "red" }}>{getMessage("errorMessage")}</div>
            </div>
        </div>
    )
}

export default FlowControls;