import React, {useMemo} from "react"
import {useSelector} from "react-redux"
import {useRouteMatch} from "react-router"
import {getTabs} from "../reducers/selectors/settings"
import {DynamicTab} from "./DynamicTab"
import NotFound from "./errors/NotFound"

export function CustomTab(): JSX.Element {
  const customTabs = useSelector(getTabs)
  const {params} = useRouteMatch<{id: string}>()
  const tab = useMemo(
    () => customTabs.find(tab => tab.id == params.id),
    [customTabs, params],
  )

  return tab ?
    (
      <div className="Page">
        <DynamicTab tab={tab}/>
      </div>
    ) :
    (
      <NotFound/>
    )
}

export const CustomTabPath = "/customtabs"
