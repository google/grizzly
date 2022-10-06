import {config} from "./StyleUtils"


export function getColorHelpText() {
    return (
        <fieldset>
            <b>Table Color Legend:</b><br/>
            <table>
                <tbody>
                    <tr><td style={{background: config.colors.PhysicalTableColor}}>Physical Tables</td></tr>
                    <tr><td style={{background: config.colors.VirtualTableColor}}>Virtual Tables</td></tr>
                    <tr><td style={{background: config.colors.ExternalTableColor}}>External Tables</td></tr>
                    <tr><td style={{background: config.colors.CycleBreakerTableColor}}>Cycle Breaker Tables</td></tr>
                </tbody>
            </table>
        </fieldset>
    )
}