export const biTools = ['Mode', 'Tableau', 'Metabase'] as const;
export type BiTool = typeof biTools[number];

export const parseBiTool = (
  biTool: string
): BiTool => {
  const identifiedElement = biTools.find(
    (element) => element.toLowerCase() === biTool.toLowerCase()
  );
  if (identifiedElement) return identifiedElement;
  throw new Error('Provision of invalid type');
};