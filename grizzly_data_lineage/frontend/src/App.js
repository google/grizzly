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

import axios from 'axios';
import { useEffect, useState } from 'react';
import { ReactFlowProvider } from 'react-flow-renderer';
import Flow from "./Flow";
import "./styles.css";

export default function App() {
  const [loading, setLoading] = useState(true);
  const [debug, setDebug] = useState(false);

  useEffect(() => {
    axios.get("/debug").then((response) => {
      setDebug(response.data === "True")
    }).catch((error) => {
      console.log(error);
    })
    setLoading(false);
  }, [])

  if (loading) {
    return <p>Loading...</p>
  }
  return (
    <div className="App">
      <ReactFlowProvider>
        <Flow debug={debug}/>
      </ReactFlowProvider>
    </div>
  );
}
