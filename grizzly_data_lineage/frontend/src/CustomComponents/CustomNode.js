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

import React, { memo } from 'react';
import { Handle } from 'react-flow-renderer';

const CustomNode = memo(({ id, data, selected }) => {
    const sourceHandle = (
        <Handle
            type="source"
            position="right"
            isConnectable={false}
        />
    )
    const targetHandle = (
        <Handle
            type="target"
            position="left"
            isConnectable={false}
        />
    )

    const dragHandleStyle = {
        display: 'inline-block',
        width: 20,
        height: 20,
        backgroundColor: 'gray',
        borderRadius: '50%',
        float: "right"
      };
    const dragHandle = (<span className="custom-drag-handle" style={dragHandleStyle} />)
    const textStyle = {
        margin: "0 auto",
    };
    const hasDescription = Object.hasOwn(data, "description");
    const description = hasDescription ? data.description : "";
    const truncatedDescription = data.truncated_description;

    return (
        <>
            {data.hasOutboundConnection && sourceHandle}
            <div title={description} style={{whiteSpace: "pre", alignItems: 'center'}}>
                {data.draggable && dragHandle}
                <b style={textStyle}>{data.label}</b><br/>
                {data.draggable && <br/>}
                {hasDescription && <p style={textStyle}>{truncatedDescription}</p>}
            </div>
            {data.hasInboundConnection && targetHandle}
        </>
    );
});

export default CustomNode;